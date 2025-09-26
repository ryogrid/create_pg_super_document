# AddStmtToCache

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:491-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L491-L552)

## Overview
Adds a new prepared statement entry to the ECPG statement cache, handling cache allocation, collision resolution, and LRU replacement.

## Definition
```c
static int AddStmtToCache(int lineno, const char *stmtID, const char *connection, int compat, const char *ecpgQuery)
```

## Detailed Description
This function manages the insertion of prepared statements into the ECPG cache system. It first allocates the cache array if not already done, then uses HashStmt to determine the appropriate bucket. The function searches for an unused entry within the bucket, but if none is found, it implements a Least Recently Used (LRU) replacement policy to evict the least frequently executed statement. Before adding the new entry, it ensures the target slot is properly freed, then populates all necessary fields including the statement ID, connection, query text, and execution counters.

## Parameters / Member Variables
- `lineno`: Line number of the statement for debugging and error reporting
- `stmtID`: Unique identifier for the prepared statement
- `connection`: Database connection name associated with the statement
- `compat`: Compatibility level flag for statement handling
- `ecpgQuery`: The ECPG-format SQL query text to be cached

## Dependencies
- Functions called/Symbols referenced:
  - stmtCacheEntry (struct type)
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - stmtCacheArraySize
  - [HashStmt](../H/HashStmt.md)
  - stmtCacheEntPerBucket
  - [ecpg_freeStmtCacheEntry](../e/ecpg_freeStmtCacheEntry.md)
  - [ecpg_strdup](../e/ecpg_strdup.md)
- Called from (representative examples):
  - [ecpg_auto_prepare](../e/ecpg_auto_prepare.md)

## Notes and Other Information
- Returns the cache entry number on success, or negative error code on failure
- Implements lazy cache allocation - only allocates memory when first needed
- Uses LRU replacement policy when the cache bucket is full
- Initializes execution counter to 0 for new entries
- Handles memory allocation failures gracefully
- Static function - only accessible within the prepare.c compilation unit
- Essential component of the automatic statement preparation system
- Works with fixed-size hash buckets to manage cache collisions