# initPopulateTable

## Location
src/bin/pgbench/pgbench.c: 4955 - 5084

## Overview
The  function populates a pgbench table with test data using PostgreSQL's COPY protocol, providing progress reporting and optimized data loading with freeze option on supported server versions.

## Definition


## Detailed Description
This function is responsible for efficiently populating pgbench tables with large amounts of test data. It uses PostgreSQL's COPY protocol for high-performance bulk data loading and provides detailed progress reporting. The function automatically uses the COPY FREEZE optimization on PostgreSQL 14+ for all tables except partitioned pgbench_accounts tables. It includes sophisticated progress reporting with time estimates and proper terminal handling for clean display updates.

## Parameters / Member Variables
- : Active PostgreSQL database connection handle
- : Name of the table to populate (e.g., "pgbench_accounts", "pgbench_branches")  
- : Base number of records per scale unit
- : Function pointer to the row initialization function (e.g., initAccount, initBranch)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer/termPQExpBuffer: PostgreSQL buffer management functions
  - PQserverVersion: Gets PostgreSQL server version for feature detection
  - PQexec: Executes the COPY statement
  - PQputline/PQendcopy: COPY protocol functions for data streaming
  - pg_time_now/PG_TIME_GET_DOUBLE: Time measurement utilities for progress reporting
  - pg_snprintf: Safe string formatting
  - PGRES_COPY_IN: Result status constant
  - LOG_STEP_SECONDS: Constant for progress reporting intervals
- Called from (representative examples):
  - initGenerateDataClientSide: Uses this function to populate branches, tellers, and accounts tables

## Notes and Other Information
- Automatically detects PostgreSQL 14+ and uses COPY FREEZE for better performance
- COPY FREEZE is disabled for partitioned pgbench_accounts tables due to limitations
- Provides two progress reporting modes: verbose (every 100k rows) and quiet (time-based intervals)
- Handles terminal output properly with carriage returns for live updates
- Supports cancellation via CancelRequested flag
- Uses efficient COPY protocol instead of individual INSERT statements
- Progress reporting includes elapsed time and estimated remaining time
- Properly cleans up terminal output formatting when complete