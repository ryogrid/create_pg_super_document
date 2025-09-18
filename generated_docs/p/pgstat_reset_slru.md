# pgstat_reset_slru

## Location
src/backend/utils/activity/pgstat_slru.c: 45 - 58

## Overview
Resets all statistical counters for a single SLRU (Simple Least Recently Used) buffer cache, providing a way to clear accumulated performance metrics for specific SLRU instances.

## Definition
```c
void pgstat_reset_slru(const char *name)
```

## Detailed Description
This function serves as the public interface for resetting SLRU statistics counters. It takes an SLRU name identifier and resets all associated performance counters (such as page hits, reads, writes, and other metrics) for that specific SLRU instance. The function includes permission checking through PostgreSQL's GRANT system to ensure only authorized users can reset statistics.

The function operates by first obtaining the current timestamp and then calling the internal reset function with the SLRU index corresponding to the provided name. This design separates the public API from the internal implementation details.

## Parameters / Member Variables
- `name`: A string identifier specifying which SLRU instance to reset. Must not be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pgstat_get_slru_index](pgstat_get_slru_index.md)  
  - [pgstat_reset_slru_counter_internal](pgstat_reset_slru_counter_internal.md)
- Called from (representative examples):
  - [pg_stat_reset_slru](pg_stat_reset_slru.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- Permission checking is managed through PostgreSQL's normal GRANT system
- The function includes an assertion to ensure the name parameter is not NULL
- This is part of PostgreSQL's statistics collection infrastructure for SLRU buffer management
- Located in src/backend/utils/activity/pgstat_slru.c:45-58