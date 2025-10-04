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
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
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

## Simplified Source

```c
static void forget_invalid_pages_db(Oid dbid) {
    HASH_SEQ_STATUS status;
    xl_invalid_page *hentry;

    // Nothing to do if invalid page table doesn't exist
    if (invalid_page_tab == NULL)
        return;

    // Iterate through all entries in the hash table
    hash_seq_init(&status, invalid_page_tab);
    while ((hentry = (xl_invalid_page *) hash_seq_search(&status)) != NULL) {
        // Check if this entry belongs to the target database
        if (hentry->key.locator.dbOid == dbid) {
            // Optional debug logging
            if (message_level_is_interesting(DEBUG2)) {
                char *path = relpathperm(hentry->key.locator, hentry->key.forkno);
                elog(DEBUG2, "page %u of relation %s has been dropped",
                     hentry->key.blkno, path);
                pfree(path);
            }

            // Remove the entry from hash table
            if (hash_search(invalid_page_tab, &hentry->key, HASH_REMOVE, NULL) == NULL)
                elog(ERROR, "hash table corrupted");
        }
    }
}
```

**Simplified Logic:**
1. **Early Exit**: Returns immediately if the invalid page hash table doesn't exist
2. **Sequential Scan**: Iterates through all entries in the invalid page hash table
3. **Database Match**: Checks if each entry belongs to the target database by comparing OIDs
4. **Optional Logging**: Logs dropped pages at DEBUG2 level when appropriate
5. **Remove Entry**: Removes matching entries from the hash table with error checking