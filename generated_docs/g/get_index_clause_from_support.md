# get_index_clause_from_support

## Location
[src/backend/optimizer/path/indxpath.c:2557-2622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2557-L2622)

## Overview
Creates an IndexClause by leveraging a function's planner support function to generate optimized index conditions for query optimization.

## Definition

```c
static IndexClause *
get_index_clause_from_support(PlannerInfo *root,
							  RestrictInfo *rinfo,
							  Oid funcid,
							  int indexarg,
							  int indexcol,
							  IndexOptInfo *index)
```
## Detailed Description
This function serves as a bridge between PostgreSQL's query planner and custom planner support functions. When a function has an associated planner support function, this routine constructs a SupportRequestIndexCondition request and calls the support function to generate index-optimized query conditions. The support function can analyze the original function call and produce equivalent index scan conditions that are more efficient than a sequential scan with function evaluation.

The function initializes a support request structure with query context, function details, and index information, then invokes the support function. If the support function successfully generates index conditions, they are wrapped in RestrictInfo nodes and packaged into an IndexClause for use by the query planner.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and global information
- : RestrictInfo containing the original restriction clause with the function call
- : OID of the function for which we're seeking index optimization support
- : Argument position within the function that corresponds to the indexed column
- : Column number within the index being considered for this optimization
- : IndexOptInfo structure containing metadata about the target index

## Dependencies
- Functions called/Symbols referenced:
  - [get_func_support](get_func_support.md)
  - OidFunctionCall1
  - make_simple_restrictinfo
  - makeNode
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [match_opclause_to_indexcol](../m/match_opclause_to_indexcol.md)
  - [match_funcclause_to_indexcol](../m/match_funcclause_to_indexcol.md)

## Notes and Other Information
- Returns NULL if the function has no planner support or if the support function cannot generate useful index conditions
- The support function API expects bare expression clauses, which this function wraps in RestrictInfo nodes
- The lossy flag defaults to true, but support functions can override this to indicate exact matches
- This mechanism allows custom data types and operators to provide sophisticated index optimization strategies
- The generated IndexClause integrates seamlessly with PostgreSQL's cost-based query optimization framework

## Simplified Source

```c
static IndexClause *
get_index_clause_from_support(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             Oid funcid,
                             int indexarg,
                             int indexcol,
                             IndexOptInfo *index)
{
    Oid prosupport = get_func_support(funcid);
    SupportRequestIndexCondition req;
    List *sresult;

    if (!OidIsValid(prosupport))
        return NULL;

    // Set up request structure for support function
    req.type = T_SupportRequestIndexCondition;
    req.root = root;
    req.funcid = funcid;
    req.node = (Node *) rinfo->clause;
    req.indexarg = indexarg;
    req.index = index;
    req.indexcol = indexcol;
    req.opfamily = index->opfamily[indexcol];
    req.indexcollation = index->indexcollations[indexcol];
    req.lossy = true;  // Default assumption

    // Call the support function to generate index conditions
    sresult = (List *) DatumGetPointer(OidFunctionCall1(prosupport,
                                                       PointerGetDatum(&req)));

    if (sresult != NIL) {
        IndexClause *iclause = makeNode(IndexClause);
        List *indexquals = NIL;
        ListCell *lc;

        // Wrap each condition in a RestrictInfo node
        foreach(lc, sresult) {
            Expr *clause = (Expr *) lfirst(lc);
            indexquals = lappend(indexquals,
                               make_simple_restrictinfo(root, clause));
        }

        // Build the IndexClause result
        iclause->rinfo = rinfo;
        iclause->indexquals = indexquals;
        iclause->lossy = req.lossy;
        iclause->indexcol = indexcol;
        iclause->indexcols = NIL;

        return iclause;
    }

    return NULL;
}
```