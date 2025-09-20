# remove_useless_joins

## Location
[src/backend/optimizer/plan/analyzejoins.c:64-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L64-L127)

## Overview
Identifies and removes relations that don't actually need to be joined in a query, optimizing the query plan by eliminating unnecessary left joins.

## Definition

```c
List *
remove_useless_joins(PlannerInfo *root, List *joinlist)
```
## Detailed Description
This function is part of PostgreSQL's query optimizer that performs join elimination optimization. It scans through the list of special join information (join_info_list) to identify left joins that can be safely removed without affecting the query result. The function only considers left-joined relations since they are the only type of joins that can be eliminated without changing semantics.

The function operates by:
1. Iterating through all SpecialJoinInfo structures in the planner's join_info_list
2. Checking each join for removability using join_is_removable()
3. When a removable join is found, extracting the inner relation ID and removing it from both the query structure and joinlist
4. Restarting the scan to ensure all removable joins are found (removal of one join may make others removable)

The optimization is particularly important for queries with views or subqueries that may introduce unnecessary joins.

## Parameters / Member Variables
- : PlannerInfo structure containing all planner state and context information
- : List representing the current join structure of the query

## Dependencies
- Functions called/Symbols referenced:
  - [join_is_removable](../j/join_is_removable.md): Determines if a specific join can be safely removed
  - [bms_singleton_member](../b/bms_singleton_member.md): Extracts single member from a bitmap set
  - [remove_rel_from_query](remove_rel_from_query.md): Removes a relation from the overall query structure
  - [remove_rel_from_joinlist](remove_rel_from_joinlist.md): Removes a relation from the joinlist structure
  - [list_delete_cell](../l/list_delete_cell.md): Removes a cell from a linked list
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md): Structure containing information about special joins

- Called from (representative examples):
  - [query_planner](../q/query_planner.md): Main query planning function that orchestrates optimization phases

## Notes and Other Information
- Currently only works with left joins where the right-hand side is a single base relation
- Uses a restart mechanism to ensure all possible removals are found, as removing one join may enable removal of others
- The function modifies both the joinlist parameter and the root->join_info_list structure
- This optimization can significantly improve query performance by reducing the number of relations that need to be processed during execution