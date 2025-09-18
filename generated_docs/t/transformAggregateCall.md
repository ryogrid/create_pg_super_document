# transformAggregateCall

## Location
src/backend/parser/parse_agg.c: 104 - 259

## Overview
Finishes the initial transformation of an aggregate function call during SQL parsing by setting up argument lists, ordering specifications, and DISTINCT processing for both regular and ordered-set aggregates.

## Definition
void transformAggregateCall(ParseState *pstate, Aggref *agg, List *args, List *aggorder, bool agg_distinct)

## Detailed Description
This function completes the transformation of an aggregate function call that was initially recognized by parse_func.c. It handles the separation of arguments into direct and aggregated args, converts regular arguments into a target list with TargetEntry nodes, and transforms ORDER BY and DISTINCT specifications into appropriate SortGroupClause lists. The function also determines the proper query nesting level for the aggregate and marks the parse state accordingly.

For ordered-set aggregates (like percentile functions), it splits the argument list between direct arguments and aggregated arguments. For regular aggregates, all arguments become part of the aggregated argument list. The function also handles ORDER BY clauses by potentially adding resjunk expressions to the target list and processes DISTINCT specifications with proper validation.

## Parameters / Member Variables
- : Current parse state containing context information for the query being parsed
- : The Aggref node representing the aggregate function call to be completed
- : List of argument expressions that have been through type coercion
- : List of SortBy nodes specifying ORDER BY clause (not yet transformed)
- : Boolean indicating whether DISTINCT was specified

## Dependencies
- Functions called/Symbols referenced:
  - AGGKIND_IS_ORDERED_SET
  - [list_copy_tail](../l/list_copy_tail.md)
  - [list_truncate](../l/list_truncate.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [addTargetToSortList](../a/addTargetToSortList.md)
  - [transformSortClause](transformSortClause.md)
  - [transformDistinctClause](transformDistinctClause.md)
  - [get_sortgroupclause_expr](../g/get_sortgroupclause_expr.md)
  - [check_agglevels_and_constraints](../c/check_agglevels_and_constraints.md)
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [transformJsonAggConstructor](transformJsonAggConstructor.md)

## Notes and Other Information
- The function validates that DISTINCT aggregates have sortable input types by checking for valid sort operators
- For ordered-set aggregates, direct arguments are stored separately from aggregated arguments
- The function modifies p_next_resno during ORDER BY processing to properly number new target list entries
- Resjunk entries added by ORDER BY/DISTINCT processing are ignored when building the final argument type list