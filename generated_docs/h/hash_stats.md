# hash_stats

## Location
src/backend/utils/hash/dynahash.c: 885 - 911

## Overview
Prints debugging statistics about hash table usage, including access counts, collision counts, and table structure information.

## Definition


## Detailed Description
This function outputs detailed statistics about a hash table to stderr for debugging purposes. It is compiled conditionally based on the HASH_STATISTICS macro. The function reports both per-table statistics (stored in the table's control structure) and global statistics maintained across all hash operations. This is primarily used for performance analysis and debugging of hash table behavior.

## Parameters / Member Variables
- : A descriptive string identifying the location or context where statistics are being printed
- : Pointer to the HTAB structure for which statistics should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - hash_get_num_entries
  - HTAB (hash table structure)
- Called from (representative examples):
  - hash_destroy

## Notes and Other Information
- Only compiled when HASH_STATISTICS is defined
- Outputs statistics to stderr using fprintf
- Reports both individual table statistics (accesses, collisions, entries, key size, max bucket, segment count) and global statistics (total accesses, collisions, expansions)
- Global statistics variables (hash_accesses, hash_collisions, hash_expansions) are maintained across all hash operations
- Primarily used for debugging and performance tuning purposes