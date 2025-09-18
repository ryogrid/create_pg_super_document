# get_sortgroupclause_expr

## Location
src/backend/optimizer/util/tlist.c: 379 - 391

## Overview
Finds the targetlist entry matching the given SortGroupClause by ressortgroupref and returns its expression.

## Definition
Node *get_sortgroupclause_expr(SortGroupClause *sgClause, List *targetList)

## Detailed Description
This function builds upon get_sortgroupclause_tle() to extract the actual expression from a TargetEntry that corresponds to a SortGroupClause. It first locates the TargetEntry using get_sortgroupclause_tle(), then returns the expr field from that TargetEntry. This is particularly useful when the optimizer needs to work with the actual expression node rather than the complete TargetEntry structure.

## Parameters / Member Variables
- `sgClause`: Pointer to a SortGroupClause structure containing the sort/group reference information
- `targetList`: List of TargetEntry nodes to search within

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupclause_tle](get_sortgroupclause_tle.md)
  - SortGroupClause (structure type)
- Called from (representative examples):
  - [make_pathkeys_for_sortclauses_extended](../m/make_pathkeys_for_sortclauses_extended.md)
  - [get_sortgrouplist_exprs](get_sortgrouplist_exprs.md)
  - [transformAggregateCall](../t/transformAggregateCall.md)
  - [transformWindowDefinitions](../t/transformWindowDefinitions.md)

## Notes and Other Information
This function provides a convenient way to extract expressions from sort/group clauses during query planning and parsing. It's commonly used when building path keys and when processing aggregate and window function calls where the system needs access to the underlying expressions for further analysis or transformation.