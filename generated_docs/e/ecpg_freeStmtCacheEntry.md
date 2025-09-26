# ecpg_freeStmtCacheEntry

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 451 - 490

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
  - prepared_statement (struct type)
  - ecpg_get_connection
  - ecpg_find_prepared_statement
  - deallocate_one
  - ecpg_free
- Called from (representative examples):
  - AddStmtToCache

## Notes and Other Information
- Returns the entry number on success, or negative error code on failure
- Returns 0 if the entry is not currently in use (no error)
- Fails safely if the statement cache is not initialized
- Handles the complete lifecycle cleanup including both cache and prepared statement cleanup
- Static function - only accessible within the prepare.c compilation unit
- Essential for cache management to prevent memory leaks and stale entries
- Works in conjunction with the prepared statement management system