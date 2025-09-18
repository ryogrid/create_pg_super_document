# BackgroundWorkerStateChange

## Location
src/backend/postmaster/bgworker.c: 246 - 431

## Overview
Processes changes to background worker state in shared memory, handling new worker registrations and termination requests while running in the postmaster process.

## Definition


## Detailed Description
This critical function runs in the postmaster process to synchronize the postmaster's private worker list with changes made to shared memory by other backends. It iterates through all worker slots in shared memory, detecting newly registered workers or workers marked for termination. The function employs defensive programming practices to handle potentially corrupted shared memory safely, as a rogue backend could compromise the postmaster if trust is placed in shared memory contents. For new workers, it validates and copies registration data to the postmaster's private list using safe string copying functions. For terminated workers, it handles cleanup and notification procedures.

## Parameters / Member Variables
- : Boolean flag controlling whether new worker registrations should be accepted (false during shutdown scenarios)

## Dependencies
- Functions called/Symbols referenced:
  -  (locates workers by slot number)
  -  (memory barrier for safe shared memory reads)
  -  (full memory barrier)
  -  (sends signals to processes)
  -  (reports worker status)
  -  (safe memory allocation)
  -  (safe string copying)
  -  (validates notification PIDs)
  -  (adds workers to private list)
  -  and  (error reporting)
  -  (memory copying)
  - Constants: , , , , 
  - Memory context flags: , 
  - Global variables: , , , 

- Called from (representative examples):
  -  (src/backend/postmaster/postmaster.c:3771)

## Notes and Other Information
- Implements defensive programming against shared memory corruption
- Uses memory barriers to ensure proper ordering of shared memory operations
- Handles parallel worker tracking with separate terminate count
- Validates notification PIDs against known backend processes
- Employs safe string copying to prevent buffer overflows from corrupted data
- Logs worker registration events at DEBUG1 level
- Critical for postmaster's worker lifecycle management
- Only allocates memory when necessary and handles allocation failures gracefully
- Maintains consistency between shared memory slots and private worker list