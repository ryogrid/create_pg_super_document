# log_invalid_page

## Location
[src/backend/access/transam/xlogutils.c:102-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L102-L165)

## Overview
Logs references to invalid pages during WAL replay and maintains a hash table to track these invalid page references for later verification.

## Definition
```c
static void log_invalid_page(RelFileLocator locator, ForkNumber forkno, BlockNumber blkno, bool present)
```

## Detailed Description
The `log_invalid_page` function is a critical component of PostgreSQL's WAL recovery mechanism that handles references to invalid pages. It maintains a hash table (`invalid_page_tab`) to track all invalid page references encountered during recovery. The function has different behaviors depending on the recovery state: if consistency has been reached, it either panics or warns about invalid references; otherwise, it logs them for debugging and stores them in the hash table for later validation. This mechanism allows PostgreSQL to detect and handle corrupted or missing pages during recovery operations.

## Parameters / Member Variables
- `locator`: RelFileLocator structure identifying the relation (tablespace, database, relation OID)
- `forkno`: Fork number indicating which fork of the relation (main, FSM, VM, etc.)
- `blkno`: Block number of the invalid page
- `present`: Boolean flag indicating if the page exists but is uninitialized (true) or doesn't exist (false)

## Dependencies
- Functions called/Symbols referenced:
  - [report_invalid_page](../r/report_invalid_page.md)
  - elog
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [xl_invalid_page_key](../x/xl_invalid_page_key.md)
  - [xl_invalid_page](../x/xl_invalid_page.md)
  - [HASHCTL](../H/HASHCTL.md)
- Called from (representative examples):
  - [XLogReadBufferExtended](../X/XLogReadBufferExtended.md)

## Notes and Other Information
- This is a static function, accessible only within xlogutils.c
- The function creates the invalid page hash table on first use with 100 initial buckets
- When reachedConsistency is true, invalid page references cause PANIC unless ignore_invalid_pages is set
- Debug-level logging is conditionally performed based on message_level_is_interesting(DEBUG1)
- The hash table uses HASH_ELEM and HASH_BLOBS flags for efficient key-based lookups
- Duplicate invalid page references are handled gracefully - the 'present' flag is preserved from the first occurrence
- The function is essential for PostgreSQL's crash recovery and standby server operations