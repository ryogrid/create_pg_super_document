# set_operation_ordered_results_useful

## Location
[src/backend/optimizer/prep/prepunion.c:188-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L188-L229)

## Overview
Determines whether a given set operation can benefit from sorted input paths to optimize execution performance.

## Definition

```c
bool
set_operation_ordered_results_useful(SetOperationStmt *setop)
```
## Detailed Description
 is a utility function that analyzes a SetOperationStmt to determine if the optimizer should consider using sorted input paths for more efficient execution. This function helps the query planner decide whether to invest resources in maintaining sort order through the set operation.

The function implements specific logic for different set operation types:
- For UNION (non-ALL): Returns true because sorted paths can be efficiently processed using MergeAppend followed by Unique operations to eliminate duplicates
- For UNION ALL: Returns false because sorted paths provide no advantage over unsorted paths since no duplicate elimination is needed
- For EXCEPT, EXCEPT ALL, INTERSECT, and INTERSECT ALL: Returns false because the current implementation cannot effectively utilize pre-sorted input paths

This optimization decision impacts how the query planner builds execution paths, potentially leading to more efficient query execution when sorted inputs are beneficial.

## Parameters / Member Variables
- : Pointer to a SetOperationStmt structure representing the set operation to analyze, containing operation type and modifier flags

## Dependencies
- Functions called/Symbols referenced:
  - SETOP_UNION (enum constant)
  - [SetOperationStmt](../S/SetOperationStmt.md) (structure access)
- Called from (representative examples):
  - [standard_qp_callback](standard_qp_callback.md) (src/backend/optimizer/plan/planner.c:3631)

## Notes and Other Information
- This function currently only optimizes UNION operations without the ALL modifier
- The limitation for EXCEPT/INTERSECT operations represents a potential area for future optimization improvements
- The function directly examines the  flag and  field of the SetOperationStmt to make its determination
- This is a relatively simple but important optimization decision that can significantly impact query performance for large set operations
- The MergeAppend + Unique optimization for UNION can be much more efficient than hash-based approaches when dealing with pre-sorted data