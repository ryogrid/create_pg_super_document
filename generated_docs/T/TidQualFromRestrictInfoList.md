# TidQualFromRestrictInfoList

## Location
[src/backend/optimizer/path/tidpath.c:280-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L280-L386)

## Overview
A static recursive function that extracts CTID (Current Tuple ID) conditions from a list of RestrictInfos with implicit AND semantics, returning OR'ed CTID qualifications suitable for TID scan optimization.

## Definition
```c
static List *TidQualFromRestrictInfoList(PlannerInfo *root, List *rlist, RelOptInfo *rel)
```

## Detailed Description
TidQualFromRestrictInfoList processes complex boolean expressions containing potential TID qualifications, handling both AND and OR clause combinations recursively. The function implements sophisticated logic to extract the most efficient set of CTID conditions for direct tuple access optimization.

Key behavioral aspects:
- **CurrentOfExpr Priority**: If a CurrentOfExpr clause is found, it immediately returns only that clause, as the executor requires exclusive handling of cursor-based access
- **OR Clause Processing**: Recursively processes OR clauses, requiring every sub-clause to have extractable CTID conditions
- **AND Clause Handling**: Handles nested AND clauses through recursive calls
- **Preference Rules**: Applies simple heuristics when multiple options exist, preferring shorter OR'ed lists and singleton clauses over complex expressions

The function's recursive nature allows it to handle arbitrarily nested boolean expressions while maintaining the constraint that all branches of an OR clause must be TID-qualified.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing global planning context and state information
- `rlist`: List of RestrictInfo structures representing the restriction clauses to analyze
- `rel`: RelOptInfo pointer representing the target relation for TID qualification

## Dependencies
- Functions called/Symbols referenced:
  - [restriction_is_or_clause](../r/restriction_is_or_clause.md)
  - [BoolExpr](../B/BoolExpr.md) (node type)
  - [is_andclause](../i/is_andclause.md)
  - [TidQualFromRestrictInfoList](TidQualFromRestrictInfoList.md) (recursive self-call)
  - [RestrictInfoIsTidQual](../R/RestrictInfoIsTidQual.md)
  - [IsCurrentOfClause](../I/IsCurrentOfClause.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [TidQualFromRestrictInfoList](TidQualFromRestrictInfoList.md) (recursive)
  - [create_tidscan_paths](../c/create_tidscan_paths.md)

## Notes and Other Information
- Static function accessible only within tidpath.c
- Implements recursive descent parsing for boolean expressions
- Special handling for CurrentOfExpr ensures executor compatibility
- Returns NIL if no usable CTID conditions are found
- Critical component of PostgreSQL's TID scan path generation
- Uses simple preference heuristics rather than complex cost-based selection
- Handles Row Level Security (RLS) quals that may be AND'ed with CurrentOfExpr
- Returns a list with implicit OR semantics across list elements

## Simplified Source

```c
static List *TidQualFromRestrictInfoList(PlannerInfo *root, List *rlist, RelOptInfo *rel)
{
    RestrictInfo *tidclause = NULL;  // Best simple CTID qual found
    List *orlist = NIL;              // Best OR'ed CTID qual found
    ListCell *l;

    foreach(l, rlist)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);

        if (restriction_is_or_clause(rinfo))
        {
            // Process OR clause - need CTID conditions in ALL sub-clauses
            List *rlst = NIL;
            ListCell *j;

            foreach(j, ((BoolExpr *) rinfo->orclause)->args)
            {
                Node *orarg = (Node *) lfirst(j);
                List *sublist;

                // Handle AND clauses recursively, simple clauses directly
                if (is_andclause(orarg))
                {
                    List *andargs = ((BoolExpr *) orarg)->args;
                    sublist = TidQualFromRestrictInfoList(root, andargs, rel);
                }
                else
                {
                    RestrictInfo *ri = castNode(RestrictInfo, orarg);
                    if (RestrictInfoIsTidQual(root, ri, rel))
                        sublist = list_make1(ri);
                    else
                        sublist = NIL;
                }

                // If any OR branch lacks CTID quals, abandon this OR clause
                if (sublist == NIL)
                {
                    rlst = NIL;
                    break;
                }

                rlst = list_concat(rlst, sublist);
            }

            // Keep shorter OR'ed lists (simple heuristic)
            if (rlst && (orlist == NIL || list_length(rlst) < list_length(orlist)))
                orlist = rlst;
        }
        else
        {
            // Handle simple (non-OR) clauses
            if (RestrictInfoIsTidQual(root, rinfo, rel))
            {
                // CurrentOfExpr has absolute priority - return immediately
                if (IsCurrentOfClause(rinfo, rel))
                    return list_make1(rinfo);

                // Remember first simple CTID qual as fallback
                if (tidclause == NULL)
                    tidclause = rinfo;
            }
        }
    }

    // Prefer simple CTID quals over complex OR'ed lists
    if (tidclause)
        return list_make1(tidclause);
    return orlist;
}
```