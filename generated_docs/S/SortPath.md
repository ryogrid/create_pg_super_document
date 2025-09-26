# SortPath

## Location
src/include/nodes/pathnodes.h: 2199 - 2203

## Overview
SortPath represents an explicit sort operation in PostgreSQL's query planner, used when data needs to be ordered according to specific sort keys.

## Definition
```c
typedef struct SortPath
{
    Path        path;
    Path       *subpath;      /* path representing input source */
} SortPath;
```

## Detailed Description
SortPath represents an explicit sorting step in the query execution plan. It is created when the planner determines that data must be sorted to satisfy ORDER BY clauses, to enable merge joins, or to support other operations that require ordered input. The sort keys are implicitly defined by the path.pathkeys field inherited from the base Path structure.

A key constraint of SortPath is that the Sort plan node cannot perform projection - it only reorders rows without modifying their content. This means the output pathtarget must be identical to the input's pathtarget. If both sorting and projection are needed, they must be handled by separate plan nodes.

SortPath competes with other path alternatives during planning, and its cost includes both the CPU cost of comparison operations and the I/O cost of potentially spilling to disk when the sort doesn't fit in memory.

## Parameters / Member Variables
- `path`: Base Path structure containing cost estimates, pathkeys (which define the sort order), and pathtarget information
- `subpath`: Pointer to the input path that provides the unsorted data to be sorted

## Dependencies
- Functions called/Symbols referenced:
  - Path (inherited base structure)
- Called from (representative examples):
  - create_sort_path (path creation)
  - create_sort_plan (plan generation)
  - create_incremental_sort_path (incremental sort optimization)

## Notes and Other Information
- The sort keys are defined by path.pathkeys rather than being stored separately in the SortPath structure
- SortPath cannot perform projection - the output pathtarget must match the input pathtarget exactly
- Used extensively for ORDER BY clauses, merge join preparation, and operations requiring sorted input
- The planner considers sort costs including potential disk spilling when work_mem is exceeded
- May be optimized away if the input is already sorted according to the required pathkeys
- Often created as a fallback when more efficient sorted access methods (like index scans) are not available
- Can be superseded by IncrementalSortPath when the input is already partially sorted