# create_merge_append_path

## Location
[src/backend/optimizer/util/pathnode.c:1415-1517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1415-L1517)

## Overview
Creates a path node corresponding to a MergeAppend plan, which merges multiple pre-sorted input streams to produce a single sorted output stream.

## Definition


## Detailed Description
This function constructs a MergeAppendPath node that represents a MergeAppend operation in PostgreSQL's query execution plan. Unlike a regular Append which simply concatenates results, MergeAppend merges multiple already-sorted input streams to maintain the sort order in the output. The function calculates costs by considering whether each subpath is already adequately sorted or requires an additional Sort node.

For subpaths that are not properly sorted, the function includes the cost of inserting a Sort node. When there's only one child path with matching parallel awareness, the operation becomes a no-op and inherits the child's costs directly. The function handles the application of query-wide LIMIT when appropriate.

## Parameters / Member Variables
- : PlannerInfo context for the query being planned
- : RelOptInfo for the relation this path represents
- : List of child paths to be merged (must produce compatible sort orders)
- : Required sort ordering for the merged output
- : Set of outer relids required by this path

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (MergeAppendPath creation)
  - get_appendrel_parampathinfo
  - [bms_equal](../b/bms_equal.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [cost_sort](cost_sort.md)
  - [cost_merge_append](cost_merge_append.md)
  - PATH_REQ_OUTER
- Called from (representative examples):
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md)
  - [generate_union_paths](../g/generate_union_paths.md)

## Notes and Other Information
- MergeAppend is never parallel-aware (parallel_aware = false, parallel_workers = 0)
- All child paths must have the same parameterization (required_outer)
- For each subpath, checks if it's adequately ordered using pathkeys_contained_in()
- If a subpath needs sorting, includes the cost of a Sort node in the total calculation
- Single-child MergeAppend with matching parallel awareness becomes a no-op
- Applies query-wide LIMIT when the path represents the sole base relation
- The resulting path maintains the specified pathkeys ordering through the merge operation
- More expensive than regular Append but preserves sort order without a final sort step