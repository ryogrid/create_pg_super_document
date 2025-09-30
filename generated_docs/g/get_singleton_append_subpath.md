# get_singleton_append_subpath

## Location
[src/backend/optimizer/path/allpaths.c:2132-2165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2132-L2165)

## Overview
Extracts the single subpath from an Append or MergeAppend path node, or returns the original path if it doesn't contain exactly one subpath.

## Definition
```c
static Path *get_singleton_append_subpath(Path *path)
```

## Detailed Description
This helper function is used in the PostgreSQL query optimizer to unwrap single-element Append and MergeAppend path nodes. When an Append or MergeAppend contains only one subpath, the wrapper is often unnecessary overhead, and this function provides access to the underlying path directly. The function performs type checking using IsA() macros and uses list_length() to verify that exactly one subpath exists before extracting it with linitial(). If the input path is not an Append/MergeAppend or contains multiple subpaths, the original path is returned unchanged.

## Parameters / Member Variables
- `path`: Input Path pointer that may be an AppendPath, MergeAppendPath, or other path type. Must not be a parallel-aware path (enforced by assertion).

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - IsA (type checking macro)
  - [list_length](../l/list_length.md) (list utility function)
  - linitial (list utility function to get first element)
  - [AppendPath](../A/AppendPath.md) (struct type)
  - [MergeAppendPath](../M/MergeAppendPath.md) (struct type)
- Called from (representative examples):
  - pushdown_safe_type
  - [generate_orderedappend_paths](generate_orderedappend_paths.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (allpaths.c)
- The function includes an assertion that the input path must not be parallel-aware
- Located in src/backend/optimizer/path/allpaths.c at lines 2132-2165
- Used primarily in path optimization scenarios where single-element append operations can be simplified

## Simplified Source

```c
static Path *get_singleton_append_subpath(Path *path) {
    // Check for single-subpath AppendPath
    if (IsA(path, AppendPath)) {
        AppendPath *apath = (AppendPath *) path;
        if (list_length(apath->subpaths) == 1)
            return (Path *) linitial(apath->subpaths);
    }
    // Check for single-subpath MergeAppendPath
    else if (IsA(path, MergeAppendPath)) {
        MergeAppendPath *mpath = (MergeAppendPath *) path;
        if (list_length(mpath->subpaths) == 1)
            return (Path *) linitial(mpath->subpaths);
    }

    // Return original path if not a singleton append
    return path;
}
```