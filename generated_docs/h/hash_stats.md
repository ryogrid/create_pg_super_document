# hash_stats

## Location
[src/backend/utils/hash/dynahash.c:885-911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L885-L911)

## Overview
Prints debugging statistics about hash table usage, including access counts, collision counts, and table structure information.

## Definition

```c
void
hash_stats(const char *where, HTAB *hashp)
```
## Detailed Description
This function outputs detailed statistics about a hash table to stderr for debugging purposes. It is compiled conditionally based on the HASH_STATISTICS macro. The function reports both per-table statistics (stored in the table's control structure) and global statistics maintained across all hash operations. This is primarily used for performance analysis and debugging of hash table behavior.

## Parameters / Member Variables
- : A descriptive string identifying the location or context where statistics are being printed
- : Pointer to the HTAB structure for which statistics should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_get_num_entries](hash_get_num_entries.md)
  - [HTAB](../H/HTAB.md) (hash table structure)
- Called from (representative examples):
  - [hash_destroy](hash_destroy.md)

## Notes and Other Information
- Only compiled when HASH_STATISTICS is defined
- Outputs statistics to stderr using fprintf
- Reports both individual table statistics (accesses, collisions, entries, key size, max bucket, segment count) and global statistics (total accesses, collisions, expansions)
- Global statistics variables (hash_accesses, hash_collisions, hash_expansions) are maintained across all hash operations
- Primarily used for debugging and performance tuning purposes

## Simplified Source

```c
// Simplified version of hash_stats
void hash_stats(const char *where, HTAB *hashp) {
    #ifdef HASH_STATISTICS
    // Print per-table statistics: accesses and collisions
    fprintf(stderr, "%s: this HTAB -- accesses %ld collisions %ld\n",
            where, hashp->hctl->accesses, hashp->hctl->collisions);

    // Print table structure information: entries, key size, max bucket, segments
    fprintf(stderr, "hash_stats: entries %ld keysize %ld maxp %u segmentcount %ld\n",
            hash_get_num_entries(hashp), (long) hashp->hctl->keysize,
            hashp->hctl->max_bucket, hashp->hctl->nsegs);

    // Print global statistics: total accesses and collisions across all tables
    fprintf(stderr, "%s: total accesses %ld total collisions %ld\n",
            where, hash_accesses, hash_collisions);

    // Print global expansion count
    fprintf(stderr, "hash_stats: total expansions %ld\n", hash_expansions);
    #endif
}
```

Key simplifications made:
- Added descriptive comments for each output section
- Grouped related fprintf statements logically
- Clarified that some statistics are per-table vs global
- Maintained original logic flow and conditional compilation