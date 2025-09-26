# tuplesort_space_type_name

## Location
src/backend/utils/sort/tuplesort.c: 2604 - 2624

## Overview
Converts a TuplesortSpaceType enumeration value to a human-readable string representation indicating whether the sort operation used memory or disk storage.

## Definition
```c
const char *tuplesort_space_type_name(TuplesortSpaceType t)
```

## Detailed Description
This simple utility function provides a string representation of the space type used during a tuplesort operation. It converts the TuplesortSpaceType enumeration to user-friendly text for display in query execution reports and diagnostic output.

The function uses a straightforward conditional expression to map the space type:
- **SORT_SPACE_TYPE_DISK** → "Disk"
- **SORT_SPACE_TYPE_MEMORY** → "Memory"

The function includes an assertion to validate that only the expected enumeration values are passed, ensuring program correctness during development and debugging.

## Parameters / Member Variables
- `t`: TuplesortSpaceType enumeration value representing whether the sort used disk or memory storage

## Dependencies
- Constants referenced:
  - SORT_SPACE_TYPE_DISK
  - SORT_SPACE_TYPE_MEMORY
- Functions called:
  - Assert (for input validation)
- Called from (representative examples):
  - show_sort_info (in explain.c for EXPLAIN output)
  - show_incremental_sort_group_info (in explain.c for incremental sort reporting)

## Notes and Other Information
- Returns a const char pointer to static string literals, requiring no memory management
- Used extensively in EXPLAIN ANALYZE output to show whether sorts stayed in memory or spilled to disk
- The assertion ensures only valid TuplesortSpaceType values are processed, helping catch programming errors early
- Part of PostgreSQL's comprehensive query performance reporting system
- The string values ("Disk" vs "Memory") are designed to be immediately understandable to database users analyzing query performance
- Critical for understanding sort performance characteristics, as disk-based sorts are typically much slower than memory-based sorts