# create_upper_unique_path

## Location
[src/backend/optimizer/util/pathnode.c:3103-3154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3103-L3154)

## Overview
Creates a pathnode that represents performing an explicit Unique step on presorted input to eliminate duplicate rows.

## Definition
```c
UpperUniquePath *create_upper_unique_path(PlannerInfo *root,
                                         RelOptInfo *rel,
                                         Path *subpath,
                                         int numCols,
                                         double numGroups)
```

## Detailed Description
This function creates an UpperUniquePath node that represents a Unique operation for eliminating duplicate rows from presorted input. The operation works by scanning through sorted input and removing consecutive duplicate rows based on the first numCols columns. This is distinct from the lower-level unique operations and is specifically designed for upper-level planning contexts like DISTINCT operations.

The function requires the input to be sorted on the grouping columns (and possibly additional columns), with the first numCols pathkeys representing the columns to check for uniqueness. The cost calculation includes comparison operations for detecting duplicates, and the output row count is reduced to the estimated number of unique groups.

## Parameters / Member Variables
- `root`: PlannerInfo containing planning context and optimizer settings
- `rel`: RelOptInfo representing the parent relation associated with the result
- `subpath`: Path representing the source of presorted input data
- `numCols`: Number of leading columns to use for uniqueness comparison
- `numGroups`: Estimated number of unique groups (output row count) after duplicate elimination

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates UpperUniquePath node)
  - cpu_operator_cost (cost parameter for comparison operations)
  - [UpperUniquePath](../U/UpperUniquePath.md) (return type structure)
- Called from (representative examples):
  - [create_partial_distinct_paths](create_partial_distinct_paths.md)
  - [create_final_distinct_paths](create_final_distinct_paths.md)
  - [generate_union_paths](../g/generate_union_paths.md)

## Notes and Other Information
- Requires input to be sorted on at least the first numCols columns for efficient duplicate detection
- Preserves the sort ordering of the input since unique operations maintain relative order
- Cost model assumes all columns are compared for most tuples, which may be an overestimate
- Does not project data - uses the same pathtarget as the input subpath
- Assumes operation above joins (no parameterization) and inherits parallel safety from subpath
- The pathtype is set to T_Unique to distinguish it from other unique operations
- Primarily used for implementing DISTINCT clauses and duplicate elimination in set operations
- The use case is different enough from create_unique_path that they remain separate functions