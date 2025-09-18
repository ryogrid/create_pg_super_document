# get_sortgrouplist_exprs

## Location
src/backend/optimizer/util/tlist.c: 392 - 421

## Overview
Given a list of SortGroupClauses, builds a list of the referenced targetlist expressions.

## Definition
List *get_sortgrouplist_exprs(List *sgClauses, List *targetList)

## Detailed Description
This function processes a list of SortGroupClause structures and extracts the corresponding expressions from the target list. It iterates through each SortGroupClause in the input list, uses get_sortgroupclause_expr() to find the associated expression, and builds a new list containing all these expressions. This is commonly used when the optimizer needs to work with the actual expressions involved in sorting or grouping operations rather than the clause structures themselves.

## Parameters / Member Variables
- `sgClauses`: List of SortGroupClause structures to process
- `targetList`: List of TargetEntry nodes to search within for matching expressions

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupclause_expr](get_sortgroupclause_expr.md)
  - SortGroupClause (structure type)
  - lappend (list manipulation function)
- Called from (representative examples):
  - [get_windowclause_startup_tuples](get_windowclause_startup_tuples.md)
  - [get_number_of_groups](get_number_of_groups.md)
  - [create_partial_distinct_paths](../c/create_partial_distinct_paths.md)
  - [create_final_distinct_paths](../c/create_final_distinct_paths.md)
  - [group_by_has_partkey](group_by_has_partkey.md)

## Notes and Other Information
This function is essential for query planning operations that need to analyze groups of expressions together. It's particularly important in window function processing, distinct path creation, and group-by optimization where the planner needs to understand the collective behavior of multiple sort/group expressions. The function returns a newly allocated list that contains references to the expression nodes.