# accumulate_append_subpath

## Location
[src/backend/optimizer/path/allpaths.c:2087-2131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2087-L2131)

## Overview
Adds a subpath to the list being built for an Append or MergeAppend, with intelligent flattening of nested append structures to eliminate unnecessary intermediate nodes.

## Definition
```c
static void accumulate_append_subpath(Path *path, List **subpaths, List **special_subpaths)
```

## Detailed Description
This function implements a critical optimization for append path construction by "cutting out the middleman" when child paths are themselves Append or MergeAppend paths. Instead of creating nested append structures, it flattens them by directly adding the grandchild paths to the parent's subpath list.

The function handles several important cases:

1. **Regular Append/MergeAppend flattening**: When a child is an Append or MergeAppend path, extract its subpaths directly rather than treating it as a single subpath
2. **Parallel-aware Append handling**: When dealing with parallel-aware Append paths that contain both partial and non-partial subpaths, it can split them into separate lists for specialized handling
3. **Sort optimization**: Omitting child MergeAppend nodes effectively omits redundant sort steps, which is beneficial since parent Append paths produce unsorted results anyway

This flattening optimization reduces plan complexity and improves execution efficiency by minimizing the number of executor nodes in the final plan tree.

## Parameters / Member Variables
- `path`: Path structure to be added to the subpath collection, potentially flattened if it's an Append or MergeAppend
- `subpaths`: Double pointer to list of paths being accumulated for the parent Append/MergeAppend (modified in-place)
- `special_subpaths`: Optional double pointer to list for collecting non-partial subpaths from parallel-aware Append paths (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - AppendPath (struct type for Append path nodes)
  - MergeAppendPath (struct type for MergeAppend path nodes)
  - [list_concat](../l/list_concat.md) (concatenates two lists)
  - [list_copy_tail](../l/list_copy_tail.md) (copies list elements starting from specified position)
  - [list_copy_head](../l/list_copy_head.md) (copies first N elements of a list)
  - lappend (appends single element to list)
- Called from (representative examples):
  - [add_paths_to_append_rel](add_paths_to_append_rel.md) (when accumulating subpaths for various append path types)
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md) (when building ordered append paths)

## Notes and Other Information
- The flattening optimization prevents unnecessarily deep plan trees that would hurt execution performance
- Special handling for parallel-aware Append paths allows proper separation of partial and non-partial subpaths for mixed parallel execution
- The function preserves the semantic correctness of the append operation while optimizing the physical plan structure
- Eliminating intermediate MergeAppend nodes removes redundant sort operations when the parent will not maintain ordering anyway
- The `special_subpaths` parameter enables sophisticated parallel execution strategies by segregating different path types