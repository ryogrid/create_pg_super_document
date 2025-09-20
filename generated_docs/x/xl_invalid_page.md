# xl_invalid_page

## Location
[src/backend/access/transam/xlogutils.c:72-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L72-L76)

## Overview
A hash table entry structure that tracks invalid page references during PostgreSQL WAL replay, storing both the page identification key and additional state information.

## Definition

```c
typedef struct xl_invalid_page
{
	xl_invalid_page_key key;	/* hash key ... must be first */
	bool		present;		/* page existed but contained zeroes */
} xl_invalid_page;
```
## Detailed Description
The  structure represents a complete hash table entry in PostgreSQL's invalid page tracking system during WAL replay. It extends the  structure by adding state information about whether the invalid page actually existed (but contained zeros) or was completely missing.

This structure is used to maintain a hash table of pages that have been referenced during WAL replay but may not exist due to subsequent relation drops or truncations. The tracking helps ensure WAL replay consistency by validating that appropriate cleanup operations are found later in the replay sequence.

## Parameters / Member Variables
- : An  structure that serves as the hash key and must be the first member for proper hash table functionality. Contains the relation locator, fork number, and block number that uniquely identify the page
- : A boolean flag indicating whether the referenced page existed but contained all zeros (true) or was completely absent (false)

## Dependencies
- Functions called/Symbols referenced:
  -  (as the key member type)
- Used by (representative examples):
  -  (creates and manages entries)
  -  (removes entries during cleanup)
  -  (removes database-specific entries)
  -  (validates remaining entries at end of recovery)

## Notes and Other Information
- The  member must be first in the structure to ensure proper hash table key extraction
- Used as the value type in the global  hash table
- The  flag helps distinguish between different types of invalid page conditions during recovery
- Part of PostgreSQL's mechanism to detect incomplete or corrupted WAL sequences
- Entries are typically cleaned up when corresponding drop/truncate operations are replayed
- Any remaining entries at the end of recovery indicate potential WAL consistency issues