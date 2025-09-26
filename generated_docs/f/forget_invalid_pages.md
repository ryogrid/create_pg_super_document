# forget_invalid_pages

## Location
[src/backend/access/transam/xlogutils.c:166-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L166-L201)

## Overview
Removes entries from the invalid page hash table for pages greater than or equal to a specified block number, typically used when pages have been dropped or truncated.

## Definition
```c
static void forget_invalid_pages(RelFileLocator locator, ForkNumber forkno, BlockNumber minblkno)
```

## Detailed Description
The `forget_invalid_pages` function cleans up the invalid page hash table by removing entries for pages that are no longer relevant due to relation operations like truncation or dropping. It iterates through the entire invalid page hash table, identifies entries matching the specified relation and fork, and removes those with block numbers greater than or equal to the minimum block number. This cleanup is essential to prevent stale invalid page references from persisting in memory and potentially causing false alarms during recovery verification.

## Parameters / Member Variables
- `locator`: RelFileLocator structure identifying the relation (tablespace, database, relation OID)
- `forkno`: Fork number indicating which fork of the relation (main, FSM, VM, etc.)
- `minblkno`: Minimum block number - all invalid page entries with block numbers >= this value will be removed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - RelFileLocatorEquals
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - relpathperm
  - elog
  - [pfree](../p/pfree.md)
  - [hash_search](../h/hash_search.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [xl_invalid_page](../x/xl_invalid_page.md)
- Called from (representative examples):
  - [XLogDropRelation](../X/XLogDropRelation.md)
  - [XLogTruncateRelation](../X/XLogTruncateRelation.md)

## Notes and Other Information
- This is a static function, accessible only within xlogutils.c
- Returns early if the invalid_page_tab hash table doesn't exist
- Uses hash table sequential scanning to iterate through all entries
- Includes DEBUG2-level logging for dropped pages when appropriate log level is enabled
- Performs error checking to detect hash table corruption during removal operations
- Essential for maintaining the integrity of the invalid page tracking system during DDL operations
- Memory allocated by relpathperm is properly freed to prevent memory leaks
- The function is typically called during WAL replay of relation modification operations