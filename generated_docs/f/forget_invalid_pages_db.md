# forget_invalid_pages_db

## Location
[src/backend/access/transam/xlogutils.c:202-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L202-L234)

## Overview
Removes all invalid page entries from the hash table for a specific database, typically used when an entire database is being dropped.

## Definition
```c
static void forget_invalid_pages_db(Oid dbid)
```

## Detailed Description
The `forget_invalid_pages_db` function performs a wholesale cleanup of invalid page references for an entire database. It scans through the invalid page hash table and removes all entries where the database OID matches the specified `dbid`. This function is essential during database drop operations to ensure that no stale invalid page references remain in memory that could interfere with future recovery operations or cause spurious warnings. The function ensures clean separation between databases by completely purging all invalid page tracking data associated with the dropped database.

## Parameters / Member Variables
- `dbid`: Database OID identifying which database's invalid pages should be removed from tracking

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - relpathperm
  - elog
  - [pfree](../p/pfree.md)
  - [hash_search](../h/hash_search.md)
  - HASH_SEQ_STATUS
  - [xl_invalid_page](../x/xl_invalid_page.md)
- Called from (representative examples):
  - [XLogDropDatabase](../X/XLogDropDatabase.md)

## Notes and Other Information
- This is a static function, accessible only within xlogutils.c
- Returns early if the invalid_page_tab hash table doesn't exist, avoiding unnecessary work
- Uses hash table sequential scanning to iterate through all entries in the table
- Compares database OIDs from the RelFileLocator to identify matching entries
- Includes DEBUG2-level logging for each dropped page when appropriate log level is enabled
- Performs error checking to detect hash table corruption during removal operations
- Memory allocated by relpathperm is properly freed to prevent memory leaks
- Essential for maintaining system integrity when databases are dropped during WAL replay
- More coarse-grained than forget_invalid_pages, operating at the database level rather than relation level