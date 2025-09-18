# XLogPrefetcherIsFiltered

## Location
src/backend/access/transam/xlogprefetcher.c: 916 - 963

## Overview
Checks whether a specific block should be skipped for prefetching due to active filters that prevent access to certain relations or block ranges.

## Definition


## Detailed Description
This function implements the filtering logic that determines whether a specific block should be excluded from prefetching. It performs a two-level filtering check:

1. **Relation-specific filtering**: First checks if there's an active filter for the specific relation that covers the requested block number. This handles cases where individual relations are being created, extended, or truncated.

2. **Database-level filtering**: If no relation-specific filter applies, checks for database-level filters that affect all relations within a database. This handles cases like database creation with file-copy strategy.

The function is optimized for the common case where no filters are active by first checking if the filter queue is empty, avoiding hash table lookups when possible. When filters are present, it performs efficient hash table lookups to determine if the block should be filtered.

## Parameters / Member Variables
- : Pointer to the XLogPrefetcher structure containing the filter infrastructure
- : RelFileLocator identifying the relation (tablespace, database, relation)
- : Block number being checked for filtering

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if filter queue has any entries
  -  - Look up filters in hash table using HASH_FIND
  -  - Constant for database-level filter lookup
- Called from (representative examples):
  -  - Main prefetcher logic to check if blocks should be skipped

## Notes and Other Information
- Returns  if the block should be filtered (skipped),  if prefetching is allowed
- Uses  optimization hint assuming filter queue is usually empty
- Supports extensive debugging output via  when filters are applied
- Database-level filtering uses  and  as wildcards
- Inline function for optimal performance in the prefetching hot path
- Critical safety mechanism to prevent reading non-existent or invalid blocks during recovery