# hash_agg_check_limits

## Location
[src/backend/executor/nodeAgg.c:1856-1881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1856-L1881)

## Overview
Monitors memory usage and group count in hash aggregation operations and triggers spill mode when configured limits are exceeded to prevent out-of-memory conditions.

## Definition
```c
static void hash_agg_check_limits(AggState *aggstate)
```

## Detailed Description
This function performs runtime monitoring of hash aggregation resource consumption and determines when to transition to spill mode. It checks two critical metrics:

1. **Memory Usage**: Combines memory allocated in the hash metadata context and the hash key context to get total memory consumption
2. **Group Count**: Monitors the current number of distinct groups in the hash table

The function implements a safety mechanism by ensuring at least one group exists before triggering spill mode, guaranteeing forward progress even in edge cases. When either the memory limit or group count limit is exceeded, it calls hash_agg_enter_spill_mode() to begin writing excess data to disk.

The check is described as "imperfect" because memory allocations can occur without adding new groups (e.g., when transition state sizes grow), but it provides effective monitoring for the primary growth scenarios.

## Parameters / Member Variables
- `aggstate`: The aggregate state containing current limits, counters, and memory contexts to check

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md)
  - [MemoryContextMemAllocated](../M/MemoryContextMemAllocated.md)
  - [hash_agg_enter_spill_mode](hash_agg_enter_spill_mode.md)
- Called from (representative examples):
  - [initialize_hash_entry](../i/initialize_hash_entry.md)

## Notes and Other Information
- Called after adding new groups to the hash table to ensure immediate limit checking
- Prevents out-of-memory conditions by proactively triggering spill mode
- The "at least one group" safety check ensures the algorithm can always make progress
- Memory measurement includes both metadata and hash key storage contexts for comprehensive tracking
- This is a key component of PostgreSQL's memory-bounded hash aggregation implementation