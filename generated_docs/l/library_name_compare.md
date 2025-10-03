# library_name_compare

## Location
[src/bin/pg_upgrade/function.c:29-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/function.c#L29-L54)

## Overview
A qsort comparator function for pointers to library names used in pg_upgrade to ensure predictable ordering when checking loadable libraries.

## Definition

```c
static int
library_name_compare(const void *p1, const void *p2)
```
## Detailed Description
This function implements a specialized comparison algorithm for LibraryInfo structures during pg_upgrade operations. It sorts libraries using a three-tier comparison strategy: first by name length (shorter names first), then alphabetically for names of the same length, and finally by database array index as a tiebreaker. This specific ordering ensures that extension modules like "hstore_plpython" are sorted after their dependencies ("hstore" and "plpython"), which is critical for proper LOAD testing of transform modules during the upgrade process.

## Parameters / Member Variables
- `*p1`: Pointer to the first LibraryInfo structure to compare
- `*p2`: Pointer to the second LibraryInfo structure to compare
## Dependencies
- Functions called/Symbols referenced:
  - [LibraryInfo](../L/LibraryInfo.md) (structure type)
  - [pg_cmp_size](../p/pg_cmp_size.md) (for comparing string lengths)
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (for comparing database numbers)
  - strlen (for getting string lengths)
  - strcmp (for alphabetical comparison)
- Called from (representative examples):
  - [check_loadable_libraries](../c/check_loadable_libraries.md)

## Notes and Other Information
- This is a static function only used within src/bin/pg_upgrade/function.c
- The sorting strategy addresses a specific limitation in the PostgreSQL backend's handling of transform modules
- The predictable ordering helps ensure reliable upgrade behavior even when the backend improves its dependency handling
- Returns standard qsort comparison values: negative if p1 < p2, zero if equal, positive if p1 > p2