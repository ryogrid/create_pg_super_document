# tuplesort_method_name

## Location
[src/backend/utils/sort/tuplesort.c:2581-2603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2581-L2603)

## Overview
Converts a TuplesortMethod enumeration value to a human-readable string representation for display in query execution reports and debugging output.

## Definition
```c
const char *tuplesort_method_name(TuplesortMethod m)
```

## Detailed Description
This utility function provides a simple mapping from TuplesortMethod enumeration values to descriptive string names. It is primarily used for generating human-readable output in EXPLAIN commands and other diagnostic contexts where the sorting method needs to be displayed to users or logged.

The function performs a straightforward switch statement to map each possible sort method to its corresponding descriptive name:
- **SORT_TYPE_STILL_IN_PROGRESS** → "still in progress"
- **SORT_TYPE_TOP_N_HEAPSORT** → "top-N heapsort"
- **SORT_TYPE_QUICKSORT** → "quicksort"
- **SORT_TYPE_EXTERNAL_SORT** → "external sort"
- **SORT_TYPE_EXTERNAL_MERGE** → "external merge"

If an unknown or invalid TuplesortMethod value is passed, the function returns "unknown" as a fallback.

## Parameters / Member Variables
- `m`: TuplesortMethod enumeration value representing the sorting algorithm used

## Dependencies
- Constants referenced:
  - SORT_TYPE_STILL_IN_PROGRESS
  - SORT_TYPE_TOP_N_HEAPSORT
  - SORT_TYPE_QUICKSORT
  - SORT_TYPE_EXTERNAL_SORT
  - SORT_TYPE_EXTERNAL_MERGE
- Called from (representative examples):
  - [show_sort_info](../s/show_sort_info.md) (in explain.c for EXPLAIN output)
  - [show_incremental_sort_group_info](../s/show_incremental_sort_group_info.md) (in explain.c)

## Notes and Other Information
- Returns a const char pointer to static string literals, no memory management required
- Provides the textual representation of sort methods shown in EXPLAIN ANALYZE output
- Used extensively in PostgreSQL's query execution reporting infrastructure
- The returned strings are designed to be user-friendly and informative for database administrators and developers analyzing query performance
- Falls back to "unknown" for any unrecognized TuplesortMethod values, providing graceful degradation