# create_recursiveunion_path

## Location
[src/backend/optimizer/util/pathnode.c:3617-3661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3617-L3661)

## Overview
Creates a pathnode that represents a recursive UNION node, which combines non-recursive and recursive terms to implement Common Table Expressions (CTEs) with recursive queries.

## Definition

```c
RecursiveUnionPath *
create_recursiveunion_path(PlannerInfo *root,
						   RelOptInfo *rel,
						   Path *leftpath,
						   Path *rightpath,
						   PathTarget *target,
						   List *distinctList,
						   int wtParam,
						   double numGroups)
```
## Detailed Description
This function creates a RecursiveUnionPath node that represents the execution of recursive UNION operations in Common Table Expressions (CTEs). The leftpath provides data for the non-recursive term (base case), while the rightpath provides data for the recursive term (recursive case). The function supports both UNION and UNION ALL semantics - for UNION ALL, the distinctList is empty and numGroups is zero, while for UNION, distinctList contains SortGroupClause structures for deduplication. The wtParam identifies the work table parameter used in the recursive execution. The result is always unsorted regardless of input ordering.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo representing the parent relation associated with the result
- : Path representing the source of data for the non-recursive term (base case)
- : Path representing the source of data for the recursive term (recursive case)
- : PathTarget structure defining the target list to be computed
- : List of SortGroupClause structures for grouping/deduplication (empty for UNION ALL)
- : Integer ID of the Param representing the work table used in recursive execution
- : Double representing the estimated number of distinct groups (zero for UNION ALL)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [cost_recursive_union](cost_recursive_union.md)
- Called from (representative examples):
  - [generate_recursion_path](../g/generate_recursion_path.md) (src/backend/optimizer/prep/prepunion.c:480)

## Notes and Other Information
- For recursive UNION ALL, distinctList is empty and numGroups is zero
- For recursive UNION (with deduplication), distinctList contains SortGroupClause structures
- Assumes no parameterization (above any joins) for simplification
- Parallel safety requires both leftpath and rightpath to be parallel-safe, plus relation's consider_parallel flag
- Uses leftpath's parallel_workers count (similar to join operations)
- [RecursiveUnion](../R/RecursiveUnion.md) result is always unsorted, regardless of input path ordering
- The wtParam represents the work table parameter that enables the recursive execution mechanism
- Cost calculation is delegated to the cost_recursive_union function which handles the complexity of iterative execution
- Essential for implementing SQL standard recursive Common Table Expressions (WITH RECURSIVE)