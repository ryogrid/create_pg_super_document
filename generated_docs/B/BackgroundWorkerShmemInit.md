# BackgroundWorkerShmemInit

## Location
src/backend/postmaster/bgworker.c: 162 - 220

## Overview
Initializes the background worker shared memory structure and copies worker registration data from the postmaster's private list to shared memory.

## Definition


## Detailed Description
This function sets up the shared memory infrastructure for background workers during PostgreSQL initialization. It allocates or attaches to a shared memory segment named "Background Worker Data" and initializes the BackgroundWorkerData structure. When running as the postmaster (not under another postmaster), it copies all registered background workers from the private BackgroundWorkerList to shared memory slots, ensuring a 1-to-1 correspondence between the postmaster's list and the shared memory array. Each worker is assigned a specific slot number that enables communication between the postmaster and worker processes.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  (shared memory allocation/attachment)
  -  (calculates required memory size)
  -  (single-linked list iterator type)
  -  (macro for iterating through lists)
  -  (macro to get container from list node)
  -  (memory copy function)
  -  (shared memory slot structure)
  -  (private worker registration structure)
  -  (worker configuration structure)
  -  (constant for invalid process ID)
  - Global variables: , , , 

- Called from (representative examples):
  -  (src/backend/storage/ipc/ipci.c:328)

## Notes and Other Information
- Only initializes worker data when running as the main postmaster process ()
- Maintains generation counters and parallel worker tracking in shared memory
- Resets  to 0 to handle potential reinitializations after crashes
- Marks unused slots as not in use to prevent stale data
- Critical for enabling communication between postmaster and background worker processes
- Part of the shared memory setup phase during PostgreSQL startup