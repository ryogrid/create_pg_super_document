# launch_worker

## Location
src/backend/postmaster/autovacuum.c: 1285 - 1336

## Overview
A wrapper function for starting an autovacuum worker from the launcher that handles database list management and scheduling after worker startup.

## Definition


## Detailed Description
The  function serves as a high-level wrapper around  that manages the database scheduling list after a worker is successfully started. While  handles the actual worker creation and database selection logic,  focuses on maintaining the DatabaseList data structure that tracks when each database should next be considered for autovacuum.

After delegating the worker startup to , this function updates the database list entry for the selected database by setting its  timestamp to the current time plus  seconds. This prevents the same database from being immediately selected again and ensures fair distribution of autovacuum work across databases.

The function also handles cases where the selected database is not present in the current DatabaseList by triggering a rebuild of the entire list. This can happen with newly created databases or when the list becomes stale due to dropped databases.

## Parameters / Member Variables
- : Current timestamp used as the base for calculating the next worker schedule time

## Dependencies
- Functions called/Symbols referenced:
  -  (performs actual worker startup and database selection)
  -  (iterate through DatabaseList)
  -  (access list node data)
  -  (calculate next worker time)
  -  (move processed database to front of list)
  -  (rebuild list when database not found)

- Called from (representative examples):
  - autovacuum launcher main loop (src/backend/postmaster/autovacuum.c:710, 729)

## Notes and Other Information
- Does not return a value (void function)
- Maintains the DatabaseList ordering by moving the processed database to the head
- Handles the case where a database might not have a pgstat entry and won't be scheduled regularly
- Works in conjunction with  to provide complete worker lifecycle management
- The  parameter controls the minimum interval between worker launches for the same database
- Critical for preventing database starvation and ensuring balanced autovacuum scheduling across the cluster