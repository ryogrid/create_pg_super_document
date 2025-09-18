# fetch_search_path_array

## Location
[src/backend/catalog/namespace.c:4859-4893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4859-L4893)

## Overview
Fetches the active search path into a caller-allocated array of OIDs, excluding the temporary namespace and returning the total count of path entries.

## Definition


## Detailed Description
This function provides an efficient way to retrieve the active search path by copying namespace OIDs directly into a caller-provided array. Unlike fetch_search_path(), this function always excludes the temporary namespace and always includes implicitly-prepended namespaces, making it suitable for existing code that would want to ignore temporary namespaces anyway. The function returns the total count of namespaces in the path, which may exceed the array length if the provided buffer is too small.

The design intentionally avoids complications with temporary namespace initialization since the temp namespace is excluded from the results. This makes the function more predictable and suitable for performance-critical paths where dynamic memory allocation should be avoided.

## Parameters / Member Variables
- : Caller-allocated array of Oid values to store the search path namespaces
- : Maximum number of entries that can be stored in the sarray

## Dependencies
- Functions called/Symbols referenced:
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - foreach (macro)
  - lfirst_oid (macro)
- Called from (representative examples):
  - [make_oper_cache_key](../m/make_oper_cache_key.md)
  - RangeVarGetRelid

## Notes and Other Information
- Returns the total count of namespaces, which may be larger than sarray_len if the buffer is insufficient
- Always excludes myTempNamespace from the results for consistency
- Always includes implicitly-prepended namespaces (like pg_catalog)
- More efficient than fetch_search_path() for callers who can pre-allocate storage
- Avoids dynamic memory allocation, making it suitable for performance-critical code paths
- If count exceeds sarray_len, only the first sarray_len entries are stored but the full count is still returned
- Used primarily by parser and operator resolution code where temporary namespace exclusion is desired