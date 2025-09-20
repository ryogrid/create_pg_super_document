# cmp_lsn

## Location
[src/backend/replication/syncrep.c:738-753](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L738-L753)

## Overview
A comparator function that sorts LSN (Log Sequence Number) values in descending order for use with qsort.

## Definition

```c
structs of per-walsender data,
 * and the number of valid entries (candidate sync senders) is returned.
 * (This might be more or fewer than num_sync;
```
## Detailed Description
This utility function serves as a comparator for the standard C library  function to sort XLogRecPtr (LSN) values in descending order. It dereferences the void pointers to access the actual LSN values and uses PostgreSQL's  function to perform the comparison.

The function intentionally reverses the comparison order (comparing lsn2 to lsn1 instead of lsn1 to lsn2) to achieve descending sort order, where the largest (most recent) LSN values appear first in the sorted array.

This function is specifically designed to support quorum-based synchronous replication where finding the Nth latest LSN position requires sorting all standby positions from highest to lowest.

## Parameters / Member Variables
- : Pointer to first XLogRecPtr value to compare
- : Pointer to second XLogRecPtr value to compare

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL's 64-bit unsigned integer comparison function
- Called from:
  -  (src/backend/replication/syncrep.c:121)
  -  (src/backend/replication/syncrep.c:720-722) - Called three times for sorting write, flush, and apply arrays

## Notes and Other Information
- Returns negative value if first LSN is greater than second (descending order)
- Returns positive value if first LSN is less than second
- Returns zero if LSNs are equal
- Used exclusively for sorting LSN arrays in quorum-based synchronous replication logic
- The function signature follows the standard qsort comparator convention
- Function location: src/backend/replication/syncrep.c:738-753