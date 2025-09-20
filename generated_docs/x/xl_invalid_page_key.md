# xl_invalid_page_key

## Location
[src/backend/access/transam/xlogutils.c:65-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L65-L70)

## Overview
A structure that serves as a hash key for tracking references to invalid pages during PostgreSQL WAL (Write-Ahead Log) replay operations.

## Definition

```c
typedef struct xl_invalid_page_key
{
	RelFileLocator locator;		/* the relation */
	ForkNumber	forkno;			/* the fork number */
	BlockNumber blkno;			/* the page */
} xl_invalid_page_key;
```
## Detailed Description
The  structure is used during XLOG replay to identify pages that may no longer exist due to relation drops or truncations. This mechanism is particularly important when  is OFF, as incremental updates to non-existent pages can occur. Rather than ignoring such references, PostgreSQL tracks them and validates that appropriate drop or truncate operations are found later in the replay sequence.

This structure serves as the hash table key for the invalid page tracking system, uniquely identifying a specific page within the database through the combination of relation file locator, fork number, and block number.

## Parameters / Member Variables
- : A  that identifies the specific relation (table, index, etc.) containing the referenced page
- : A  specifying which fork of the relation (main, FSM, VM, etc.) contains the page
- : A  identifying the specific page block within the fork

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a pure data structure)
- Used by (representative examples):
  -  (as the key member)
  -  (for hash table operations)

## Notes and Other Information
- This structure is designed to work as a hash table key and assumes no padding between members for proper hash computation
- The structure is part of PostgreSQL's WAL replay consistency checking mechanism
- Used in conjunction with  to form complete hash table entries
- Critical for detecting incomplete WAL sequences during recovery operations