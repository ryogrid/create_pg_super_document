# pgstat_get_slru_name

## Location
[src/backend/utils/activity/pgstat_slru.c:118-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L118-L131)

## Overview
Returns the human-readable name of an SLRU (Simple LRU) buffer cache based on its index, with bounds checking.

## Definition


## Detailed Description
This function provides a way to retrieve the descriptive name of an SLRU cache given its numeric index. It includes bounds checking to ensure the index is valid, returning NULL for invalid indices. This design allows calling code to iterate through SLRU indices without knowing the exact number of entries in advance. The function maps SLRU indices to their corresponding names from the slru_names array, which includes entries like "commit_timestamp", "multixact_member", "multixact_offset", "notify", "serializable", "subtransaction", "transaction", and "other".

## Parameters / Member Variables
- : Integer index of the SLRU cache (should be between 0 and SLRU_NUM_ELEMENTS-1, returns NULL if out of bounds)

## Dependencies
- Functions called/Symbols referenced:
  - SLRU_NUM_ELEMENTS
- Called from (representative examples):
  - PG_STAT_GET_SLRU_COLS
  - pgstat_count_buffer_hit

## Notes and Other Information
- Returns NULL for invalid indices (negative or >= SLRU_NUM_ELEMENTS), allowing safe iteration
- The function provides a mapping from numeric SLRU indices to human-readable names
- Used primarily by SQL functions and system views to display SLRU statistics with meaningful names
- The slru_names array includes standard PostgreSQL SLRU types plus a catch-all "other" entry
- Enables dynamic discovery of available SLRU caches without hardcoding the number of elements
- Part of the PostgreSQL statistics system's user-facing interface for SLRU monitoring