# ecpg_freeStmtCacheEntry

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:451-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L451-L490)

## Overview
Frees a specific entry in the ECPG prepared statement cache, cleaning up associated prepared statements and memory.

## Definition
```c
static int ecpg_freeStmtCacheEntry(int lineno, int compat, int entNo)
```

## Detailed Description
This function handles the complete cleanup of a cached prepared statement entry. It deallocates the corresponding prepared statement from the connection, clears the cache entry fields, and frees the allocated memory for the ECPG query string. The function performs several safety checks including verifying that the cache is initialized and that the specified entry is actually in use before proceeding with the cleanup operations.

## Parameters / Member Variables
- `lineno`: Line number information for error reporting and debugging
- `compat`: Compatibility mode flag for statement deallocation
- `entNo`: The cache entry number to be freed

## Dependencies
- Functions called/Symbols referenced:
  - stmtCacheEntry (struct type)
  - [prepared_statement](../p/prepared_statement.md) (struct type)
  - [ecpg_get_connection](ecpg_get_connection.md)
  - [ecpg_find_prepared_statement](ecpg_find_prepared_statement.md)
  - [deallocate_one](../d/deallocate_one.md)
  - [ecpg_free](ecpg_free.md)
- Called from (representative examples):
  - [AddStmtToCache](../A/AddStmtToCache.md)

## Notes and Other Information
- Returns the entry number on success, or negative error code on failure
- Returns 0 if the entry is not currently in use (no error)
- Fails safely if the statement cache is not initialized
- Handles the complete lifecycle cleanup including both cache and prepared statement cleanup
- Static function - only accessible within the prepare.c compilation unit
- Essential for cache management to prevent memory leaks and stale entries
- Works in conjunction with the prepared statement management system

## Simplified Source

```c
static int ecpg_freeStmtCacheEntry(int lineno, int compat, int entNo) {
    stmtCacheEntry *entry;
    struct connection *con;
    struct prepared_statement *this, *prev;

    // Check if cache is initialized
    if (stmtCacheEntries == NULL)
        return -1;

    entry = &stmtCacheEntries[entNo];

    // Return early if entry is not in use
    if (!entry->stmtID[0])
        return 0;

    // Get the connection for this cache entry
    con = ecpg_get_connection(entry->connection);

    // Find and deallocate the prepared statement
    this = ecpg_find_prepared_statement(entry->stmtID, con, &prev);
    if (this && !deallocate_one(lineno, compat, con, prev, this))
        return -1;

    // Clear the cache entry
    entry->stmtID[0] = '\0';

    // Free the cached query memory
    if (entry->ecpgQuery) {
        ecpg_free(entry->ecpgQuery);
        entry->ecpgQuery = 0;
    }

    return entNo;
}
```