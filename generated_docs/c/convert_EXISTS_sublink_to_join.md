# convert_EXISTS_sublink_to_join

## Location
[src/backend/optimizer/plan/subselect.c:1371-1539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1371-L1539)

## Overview
Attempts to convert an EXISTS SubLink into a semi-join or anti-join, enabling the query planner to use more efficient join algorithms instead of nested loop execution for EXISTS subqueries.

## Definition

```c
JoinExpr *
convert_EXISTS_sublink_to_join(PlannerInfo *root, SubLink *sublink,
							   bool under_not, Relids available_rels)
```
## Detailed Description
This function transforms EXISTS subqueries into semi-joins (for EXISTS) or anti-joins (for NOT EXISTS) when certain conditions are met. The transformation is a key optimization that can significantly improve query performance by allowing the optimizer to consider hash joins, merge joins, and other join algorithms instead of being limited to nested loop execution.

The function performs several validation checks before attempting the conversion:
- Ensures the subquery doesn't contain WITH clauses (CTEs)
- Verifies the subquery can be simplified using 
- Checks that the subquery body doesn't reference parent query variables
- Ensures the WHERE clause contains parent query variable references
- Validates that the WHERE clause doesn't contain volatile functions

If all checks pass, it pulls up the subquery's range table into the parent query and constructs a JoinExpr node representing the semi-join or anti-join.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and state
- : The EXISTS SubLink node to be converted
- : Boolean indicating if this is a NOT EXISTS (creates anti-join) vs EXISTS (creates semi-join)
- : Relids bitmapset of relations available for joining at this point in query planning

## Dependencies
- Functions called/Symbols referenced:
  - : Simplifies the EXISTS subquery by removing unnecessary elements
  - : Creates a deep copy of the subquery for safe modification
  - : Checks for variable references at specific query nesting levels
  - : Detects volatile function calls that prevent optimization
  - : Ensures subquery has a non-empty FROM clause
  - : Adjusts variable reference numbers after range table merger
  - : Adjusts variable sublevel references
  - : Extracts variable relation IDs from expressions
  - : Merges subquery range table into parent query
- Called from (representative examples):
  - : Main entry point for sublink pullup optimization

## Notes and Other Information
- Returns NULL if the conversion is not possible due to any validation failure
- The conversion is more restrictive than  because EXISTS subqueries must be completely flattened
- Semi-joins (EXISTS) and anti-joins (NOT EXISTS) preserve the original query semantics while enabling better join algorithms
- The function assumes the outer query has no references to the inner query, which is always true for EXISTS subqueries
- Part of PostgreSQL's subquery optimization framework that transforms correlated subqueries into joins when beneficial

## Simplified Source

```c
JoinExpr *
convert_EXISTS_sublink_to_join(PlannerInfo *root, SubLink *sublink,
                              bool under_not, Relids available_rels)
{
    Query *parse = root->parse;
    Query *subselect = (Query *) sublink->subselect;
    Node *whereClause;
    int rtoffset;
    Relids clause_varnos;
    Relids upper_varnos;
    JoinExpr *result;

    // Can't flatten if subquery contains WITH clauses
    if (subselect->cteList)
        return NULL;

    // Make a safe copy of the subquery
    subselect = copyObject(subselect);

    // Simplify the EXISTS subquery (remove unnecessary targetlist, etc.)
    if (!simplify_EXISTS_query(root, subselect))
        return NULL;

    // Extract WHERE clause for conversion to join quals
    whereClause = subselect->jointree->quals;
    subselect->jointree->quals = NULL;

    // Subquery body can't reference parent query variables
    if (contain_vars_of_level((Node *) subselect, 1))
        return NULL;

    // WHERE clause must reference parent query for meaningful join
    if (!contain_vars_of_level(whereClause, 1))
        return NULL;

    // No volatile functions in WHERE clause
    if (contain_volatile_functions(whereClause))
        return NULL;

    // Ensure subquery has non-empty jointree
    replace_empty_jointree(subselect);

    // Merge subquery range table into parent query
    rtoffset = list_length(parse->rtable);
    OffsetVarNodes((Node *) subselect, rtoffset, 0);
    OffsetVarNodes(whereClause, rtoffset, 0);

    // Adjust variable sublevel references
    IncrementVarSublevelsUp((Node *) subselect, -1, 1);
    IncrementVarSublevelsUp(whereClause, -1, 1);

    // Identify upper-level variables in the WHERE clause
    clause_varnos = pull_varnos(root, whereClause);
    upper_varnos = NULL;
    int varno = -1;
    while ((varno = bms_next_member(clause_varnos, varno)) >= 0) {
        if (varno <= rtoffset)
            upper_varnos = bms_add_member(upper_varnos, varno);
    }
    bms_free(clause_varnos);

    // Check that only available relations are referenced
    if (!bms_is_subset(upper_varnos, available_rels))
        return NULL;

    // Merge the range tables
    CombineRangeTables(&parse->rtable, &parse->rteperminfos,
                      subselect->rtable, subselect->rteperminfos);

    // Build semi-join or anti-join expression
    result = makeNode(JoinExpr);
    result->jointype = under_not ? JOIN_ANTI : JOIN_SEMI;
    result->isNatural = false;
    result->larg = NULL;    // caller sets this

    // Use simplified subquery jointree as right arg
    if (list_length(subselect->jointree->fromlist) == 1)
        result->rarg = (Node *) linitial(subselect->jointree->fromlist);
    else
        result->rarg = (Node *) subselect->jointree;

    result->usingClause = NIL;
    result->join_using_alias = NULL;
    result->quals = whereClause;
    result->alias = NULL;
    result->rtindex = 0;

    return result;
}
```