# pgstat_init_wal

## Location
src/backend/utils/activity/pgstat_wal.c: 141 - 158

## Overview
Initializes the WAL statistics tracking system by establishing the baseline for WAL usage counter calculations in subsequent statistics collection cycles.

## Definition
void pgstat_init_wal(void)

## Detailed Description
This function performs the essential initialization step for WAL statistics tracking by setting the previous WAL usage counters (prevWalUsage) to the current WAL usage values (pgWalUsage). This initialization establishes the baseline that pgstat_flush_wal() will use to calculate incremental WAL usage differences.

The function is called during PostgreSQL's statistics system initialization to ensure that the first call to pgstat_flush_wal() has a valid starting point for calculating WAL usage deltas. Without this initialization, the first statistics calculation could include incorrect or potentially large values representing usage from before the statistics system was ready.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - No function calls (simple assignment operation)
- Called from (representative examples):
  - pgstat_initialize

## Notes and Other Information
- Essential for proper WAL statistics tracking initialization
- Must be called before any WAL statistics collection begins
- Ensures accurate incremental WAL usage calculations from the first collection cycle
- Part of the broader PostgreSQL statistics initialization sequence
- Simple but critical function that prevents incorrect initial statistics values
- The function establishes the reference point for all subsequent WAL usage difference calculations