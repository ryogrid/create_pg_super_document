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