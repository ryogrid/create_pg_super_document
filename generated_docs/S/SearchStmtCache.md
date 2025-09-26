# SearchStmtCache

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:415-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L415-L450)

## Overview
Searches the ECPG prepared statement cache for a matching SQL statement and returns the cache entry number if found.

## Definition
```c
static int SearchStmtCache(const char *ecpgQuery)
```

## Detailed Description
This function implements the search mechanism for the ECPG prepared statement cache. It first computes a hash value for the given SQL statement using `HashStmt`, then performs a linear search within the appropriate hash bucket to find an exact match. The search compares the full ECPG-format query string against cached entries. If no matching entry is found within the bucket, the function returns 0 (indicating not found, as the 0th cache entry is intentionally unused).

## Parameters / Member Variables
- `ecpgQuery`: The ECPG-format SQL statement to search for in the cache

## Dependencies
- Functions called/Symbols referenced:
  - HashStmt
  - stmtCacheEntPerBucket (global variable)
  - stmtCacheEntries (global cache array)
- Called from (representative examples):
  - ecpg_auto_prepare

## Notes and Other Information
- Returns 0 if the statement is not found (0th entry is never used)
- Performs a quick failure check if the cache is not initialized
- Uses linear probing within hash buckets to handle collisions
- The search is case-sensitive and requires exact string matching
- Static function - only accessible within the prepare.c compilation unit
- Part of the automatic statement preparation system in ECPG
- Cache entries are identified by non-empty stmtID fields