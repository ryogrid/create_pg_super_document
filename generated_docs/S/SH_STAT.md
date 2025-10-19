# SH_STAT

## Location
[src/include/lib/simplehash.h:1072-1146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L1072-L1146)

## Overview
A macro that expands to a hash table statistics reporting function used in PostgreSQL's simplehash system for debugging and profiling hash table performance.

## Definition
```c
#define SH_STAT SH_MAKE_NAME(stat)
```

Function signature (after macro expansion):
```c
void <prefix>_stat(<prefix>_hash *tb)
```

## Detailed Description
SH_STAT is part of PostgreSQL's generic hash table implementation template system. This macro expands to create a type-specific function that analyzes and reports comprehensive statistics about the hash table's performance characteristics. The function is intended for debugging and profiling purposes only.

The function performs detailed analysis by iterating through all hash table elements and calculating various performance metrics including:
- Fill factor (ratio of occupied slots to total slots)
- Chain length statistics (how far elements are displaced from their optimal positions)
- Collision statistics (how many elements hash to the same initial bucket)
- Distribution metrics including maximum, total, and average values

The statistics are reported through logging using either pg_log_info() or elog(LOG, ...) depending on the compilation context. All metrics help developers understand hash table efficiency and identify potential performance issues.

## Parameters / Member Variables
- `tb`: Pointer to the hash table structure to analyze and report statistics for

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for generating type-specific names)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - SH_STATUS_IN_USE (status constant for checking occupied elements)
  - [SH_ENTRY_HASH](SH_ENTRY_HASH.md) (macro for computing element hash)
  - [SH_INITIAL_BUCKET](SH_INITIAL_BUCKET.md) (macro for computing optimal bucket position)
  - [SH_DISTANCE_FROM_OPTIMAL](SH_DISTANCE_FROM_OPTIMAL.md) (macro for computing displacement distance)
  - sh_log (macro that resolves to pg_log_info or elog for output)
- Called from:
  - Primarily used for debugging and profiling purposes (no regular production usage found)

## Notes and Other Information
- Part of the simplehash.h template system that generates type-specific hash table implementations
- Intended exclusively for debugging and profiling purposes, not for production use
- Performs comprehensive performance analysis including fill factor, chain lengths, and collisions
- Uses temporary memory allocation to track collision statistics, which is freed after analysis
- Logs detailed performance metrics in a human-readable format
- The function provides insights into hash table efficiency and can help identify performance bottlenecks
- Statistics include both absolute values (max, total) and computed averages for better analysis

## Simplified Source

```c
// Macro definition
#define SH_STAT SH_MAKE_NAME(stat)

// Implementation
void SH_STAT(SH_TYPE *tb) {
    uint32 max_chain_length = 0;
    uint32 total_chain_length = 0;
    double avg_chain_length;
    double fillfactor;

    // Allocate collision tracking array
    uint32 *collisions = (uint32 *) palloc0(tb->size * sizeof(uint32));
    uint32 total_collisions = 0;
    uint32 max_collisions = 0;

    // Calculate chain lengths and collisions for each element
    for (uint32 i = 0; i < tb->size; i++) {
        SH_ELEMENT_TYPE *elem = &tb->data[i];

        if (elem->status != SH_STATUS_IN_USE)
            continue;

        uint32 hash = SH_ENTRY_HASH(tb, elem);
        uint32 optimal = SH_INITIAL_BUCKET(tb, hash);
        uint32 dist = SH_DISTANCE_FROM_OPTIMAL(tb, optimal, i);

        if (dist > max_chain_length)
            max_chain_length = dist;
        total_chain_length += dist;

        collisions[optimal]++;
    }

    // Calculate collision statistics
    for (uint32 i = 0; i < tb->size; i++) {
        uint32 curcoll = collisions[i];
        if (curcoll > 1) {  // More than one element = collision
            total_collisions += (curcoll - 1);
            if ((curcoll - 1) > max_collisions)
                max_collisions = curcoll - 1;
        }
    }

    pfree(collisions);

    // Calculate averages and fill factor
    if (tb->members > 0) {
        fillfactor = tb->members / ((double) tb->size);
        avg_chain_length = ((double) total_chain_length) / tb->members;
    } else {
        fillfactor = 0;
        avg_chain_length = 0;
    }

    // Log comprehensive statistics
    sh_log("size: %lu, members: %u, filled: %f, max chain: %u, avg chain: %f, max collisions: %u",
           tb->size, tb->members, fillfactor, max_chain_length, avg_chain_length, max_collisions);
}
```