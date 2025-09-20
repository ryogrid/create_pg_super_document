# cmp_lbestatus

## Location
[src/backend/utils/activity/backend_status.c:1049-1071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L1049-L1071)

## Overview
A static comparison function used by bsearch() to search through arrays of LocalPgBackendStatus structures by comparing their proc_number fields.

## Definition

```c
static int
cmp_lbestatus(const void *a, const void *b)
```
## Detailed Description
This is a standard comparison function that implements the comparison logic required for binary search operations on arrays of LocalPgBackendStatus structures. It follows the standard C library bsearch() convention, returning a negative value if the first argument is less than the second, zero if they are equal, and a positive value if the first argument is greater than the second. The comparison is based solely on the proc_number field of the LocalPgBackendStatus structures.

## Parameters / Member Variables
- `a`: Pointer to the first LocalPgBackendStatus structure to compare
- `b`: Pointer to the second LocalPgBackendStatus structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [LocalPgBackendStatus](../L/LocalPgBackendStatus.md) (struct type)
- Called from (representative examples):
  - [pgstat_get_local_beentry_by_proc_number](../p/pgstat_get_local_beentry_by_proc_number.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the backend_status.c file
- The function enables efficient O(log n) searches through sorted arrays of backend status entries
- Returns the difference between proc_number fields, which naturally provides the correct ordering for bsearch()
- Part of PostgreSQL's backend status tracking system for monitoring active database connections and processes