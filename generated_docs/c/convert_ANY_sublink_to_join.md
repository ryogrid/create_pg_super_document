# convert_ANY_sublink_to_join

## Location
[src/backend/optimizer/plan/subselect.c:1254-1370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1254-L1370)

## Overview
Converts an ANY SubLink expression into a semi-join (JOIN_SEMI) by pulling up the subquery into the main query's range table and transforming the sublink test expression into join qualification conditions.

## Definition

```c
JoinExpr *
convert_ANY_sublink_to_join(PlannerInfo *root, SubLink *sublink,
							Relids available_rels)
```
## Detailed Description
This function implements a critical query optimization technique in PostgreSQL by converting EXISTS-equivalent ANY sublinks into semi-joins. Semi-joins are often more efficiently executed than correlated subqueries because they can leverage hash joins, nested loops, and other join algorithms instead of repetitive subquery evaluation.

The conversion process involves several key steps:

1. **Validation checks**: Ensures the sublink is convertible by checking for volatile functions, proper variable references, and availability constraints
2. **LATERAL detection**: Determines if the subquery references parent query variables, requiring LATERAL semantics
3. **Range table integration**: Adds the subquery to the parent query's range table as a new RTE_SUBQUERY entry
4. **Variable substitution**: Converts Params in the test expression to Vars referencing the pulled-up subquery
5. **Join construction**: Creates a JoinExpr node with JOIN_SEMI type and appropriate qualification conditions

The function performs extensive safety checks to ensure the transformation preserves query semantics, particularly around variable scoping and outer join interactions. The available_rels parameter restricts which relations can be safely referenced to avoid semantic changes in complex queries with outer joins.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the current query's planner state and parse tree
- `*sublink`: The SubLink node to be converted (must be ANY_SUBLINK type)
- `available_rels`: Bitmapset of relation IDs that can safely be referenced in the converted expression (used to maintain proper semantics with outer joins)
## Dependencies
- Functions called/Symbols referenced:
  - [pull_varnos_of_level](../p/pull_varnos_of_level.md)
  - [pull_varnos](../p/pull_varnos.md)
  - bms_is_empty
  - [bms_is_subset](../b/bms_is_subset.md)
  - [contain_volatile_functions](contain_volatile_functions.md)
  - [make_parsestate](../m/make_parsestate.md)
  - [addRangeTableEntryForSubquery](../a/addRangeTableEntryForSubquery.md)
  - [makeAlias](../m/makeAlias.md)
  - makeNode
  - [generate_subquery_vars](../g/generate_subquery_vars.md)
  - [convert_testexpr](convert_testexpr.md)
  - [lappend](../l/lappend.md)
  - [list_length](../l/list_length.md)
  - ANY_SUBLINK, JOIN_SEMI (enum constants)
  - NIL (null list constant)
- Called from (representative examples):
  - [pull_up_sublinks_qual_recurse](../p/pull_up_sublinks_qual_recurse.md)

## Notes and Other Information
- Returns NULL if the sublink cannot be safely converted to a join, allowing fallback to traditional subquery processing
- The returned JoinExpr has larg set to NULL - the caller must set it to represent the left-hand relations
- Successfully converted sublinks must be removed from their original position in the query quals
- The transformation can significantly improve query performance by enabling more efficient join algorithms
- LATERAL semantics are automatically detected and applied when the subquery references outer variables
- The function adds the subquery to the range table, making it accessible for join processing
- Semi-joins preserve the semantics of ANY sublinks by ensuring each outer row matches at most once
- Volatile functions in the test expression prevent conversion to maintain consistent evaluation semantics
- The available_rels constraint is crucial for maintaining correct semantics in complex queries with multiple join levels

## Simplified Source

```c
JoinExpr *
convert_ANY_sublink_to_join(PlannerInfo *root, SubLink *sublink,
                            Relids available_rels)
{
    Query *parse = root->parse;
    Query *subselect = (Query *) sublink->subselect;
    Relids upper_varnos;
    int rtindex;
    ParseNamespaceItem *nsitem;
    RangeTblEntry *rte;
    RangeTblRef *rtr;
    List *subquery_vars;
    Node *quals;
    ParseState *pstate;
    Relids sub_ref_outer_relids;
    bool use_lateral;
    JoinExpr *result;

    // Check for LATERAL semantics (subquery references parent vars)
    sub_ref_outer_relids = pull_varnos_of_level(NULL, (Node *) subselect, 1);
    use_lateral = !bms_is_empty(sub_ref_outer_relids);

    // Validate that subquery only references available relations
    if (!bms_is_subset(sub_ref_outer_relids, available_rels))
        return NULL;

    // Test expression must contain parent query variables
    upper_varnos = pull_varnos(root, sublink->testexpr);
    if (bms_is_empty(upper_varnos))
        return NULL;

    // Test expression can only reference available relations
    if (!bms_is_subset(upper_varnos, available_rels))
        return NULL;

    // No volatile functions allowed in test expression
    if (contain_volatile_functions(sublink->testexpr))
        return NULL;

    // Add subquery to range table
    pstate = make_parsestate(NULL);
    nsitem = addRangeTableEntryForSubquery(pstate, subselect,
                                          makeAlias("ANY_subquery", NIL),
                                          use_lateral, false);
    rte = nsitem->p_rte;
    parse->rtable = lappend(parse->rtable, rte);
    rtindex = list_length(parse->rtable);

    // Create range table reference for the subquery
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = rtindex;

    // Generate variables representing subquery outputs
    subquery_vars = generate_subquery_vars(root, subselect->targetList, rtindex);

    // Convert test expression to join quals
    quals = convert_testexpr(root, sublink->testexpr, subquery_vars);

    // Build the semi-join expression
    result = makeNode(JoinExpr);
    result->jointype = JOIN_SEMI;
    result->isNatural = false;
    result->larg = NULL;        // caller sets this
    result->rarg = (Node *) rtr;
    result->usingClause = NIL;
    result->join_using_alias = NULL;
    result->quals = quals;
    result->alias = NULL;
    result->rtindex = 0;

    return result;
}
```