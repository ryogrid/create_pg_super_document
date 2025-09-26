# lwlock_stats_key

## Location
src/backend/storage/lmgr/lwlock.c: 241 - 245

## Overview
lwlock_stats_key is a structure that serves as a unique identifier key for lightweight lock statistics, combining tranche and instance information to uniquely identify specific locks for performance monitoring.

## Definition
```c
typedef struct lwlock_stats_key
{
    int         tranche;
    void       *instance;
} lwlock_stats_key;
```

## Detailed Description
lwlock_stats_key is used as a composite key structure in PostgreSQL's lightweight lock statistics system. This structure uniquely identifies a specific lock instance for statistical tracking purposes. The combination of tranche (which categorizes the type of lock) and instance (which points to the specific lock object) creates a unique identifier that allows the statistics system to track performance metrics for individual locks.

This key structure is essential for the lock monitoring and performance analysis infrastructure, enabling PostgreSQL to collect and report detailed statistics about lock usage, contention, and performance characteristics on a per-lock basis.

## Parameters / Member Variables
- `tranche`: An integer identifier representing the lock tranche (category/type of the lock)
- `instance`: A void pointer to the specific lock instance being tracked

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - lwlock_stats (structure that uses this as a key)
  - init_lwlock_stats
  - get_lwlock_stats_entry

## Notes and Other Information
- This structure is used as a key in hash tables or similar data structures for lock statistics
- The combination of tranche and instance ensures unique identification of each lock
- Part of PostgreSQL's performance monitoring infrastructure for lightweight locks
- Defined in src/backend/storage/lmgr/lwlock.c at lines 241-245
- Used by the lock statistics system to track per-lock performance metrics
- The void pointer allows flexibility in pointing to different types of lock structures