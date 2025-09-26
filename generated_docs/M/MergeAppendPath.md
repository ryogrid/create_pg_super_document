# MergeAppendPath

## Location
[src/include/nodes/pathnodes.h:1955-1960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1955-L1960)

## Overview
MergeAppendPath represents a MergeAppend plan that merges sorted results from several member plans to produce similarly-sorted output.

## Definition
```c
typedef struct MergeAppendPath
{
	Path		path;
	List	   *subpaths;		/* list of component Paths */
	Cardinality limit_tuples;	/* hard limit on output tuples, or -1 */
} MergeAppendPath;
```

## Detailed Description
MergeAppendPath is a path node that represents a MergeAppend operation in PostgreSQL query planning. It efficiently combines multiple sorted input paths into a single sorted output stream without requiring a separate sort operation. This is particularly useful for operations on partitioned tables or UNION ALL queries where the individual components are already sorted on the desired key.

The path leverages the fact that if multiple input streams are already sorted on the same key, they can be merged efficiently by comparing the current row from each stream and selecting the smallest one, similar to a merge operation in merge sort.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information like cost estimates, row count, and pathkeys
- `subpaths`: List of component Path nodes that will be merged together; each subpath should produce sorted output
- `limit_tuples`: Hard limit on output tuples (-1 if no limit); used for optimization when query has LIMIT clause

## Dependencies
- Functions called/Symbols referenced:
  - Cardinality
- Called from (representative examples):
  - [ExecSupportsMarkRestore](../E/ExecSupportsMarkRestore.md)
  - [accumulate_append_subpath](../a/accumulate_append_subpath.md)
  - [get_singleton_append_subpath](../g/get_singleton_append_subpath.md)
  - [create_plan_recurse](../c/create_plan_recurse.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)
  - [create_merge_append_path](../c/create_merge_append_path.md)
  - [get_param_path_clause_serials](../g/get_param_path_clause_serials.md)

## Notes and Other Information
- [MergeAppend](MergeAppend.md) is more efficient than Append + Sort when input paths are already sorted
- The planner automatically inserts Sort nodes for subpaths that are not adequately ordered
- Cost calculation considers whether subpaths need additional sorting
- If there is only one subpath with matching parallel awareness, the MergeAppend becomes a no-op and may be eliminated
- Particularly useful for partitioned tables where each partition is individually sorted
- All child paths must have the same parameterization