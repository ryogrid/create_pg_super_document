# AutoVacuumShmemSize

## Location
src/backend/postmaster/autovacuum.c: 3300 - 3318

## Overview
AutoVacuumShmemSize calculates the amount of shared memory space required for autovacuum-related data structures, including the main control structure and worker information arrays.

## Definition


## Detailed Description
This function computes the total shared memory space needed for the autovacuum subsystem. It calculates space for two main components:

1. The fixed AutoVacuumShmemStruct which contains the main autovacuum control data
2. An array of WorkerInfoData structures, with one entry for each possible autovacuum worker process

The calculation uses PostgreSQL's memory alignment and size computation utilities to ensure proper memory layout. The MAXALIGN macro ensures the base structure is properly aligned before adding the variable-sized worker array. The mul_size and add_size functions provide overflow-safe arithmetic for memory size calculations.

The number of worker slots is determined by the autovacuum_max_workers configuration parameter, allowing the shared memory allocation to scale with the configured maximum number of concurrent autovacuum workers.

## Parameters / Member Variables
This function takes no parameters and returns a Size value representing the required shared memory space in bytes.

## Dependencies
- Functions called/Symbols referenced:
  - sizeof() (for structure size calculation)
  - MAXALIGN() (memory alignment macro)
  - [add_size](../a/add_size.md)() (overflow-safe addition)
  - [mul_size](../m/mul_size.md)() (overflow-safe multiplication)
- Data structures referenced:
  - [AutoVacuumShmemStruct](AutoVacuumShmemStruct.md) (main autovacuum control structure)
  - [WorkerInfoData](../W/WorkerInfoData.md) (per-worker information structure)
- Global variables used:
  - autovacuum_max_workers (configuration parameter)
- Called from:
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (at src/backend/storage/ipc/ipci.c:140)
  - [AutoVacuumShmemInit](AutoVacuumShmemInit.md) (at src/backend/postmaster/autovacuum.c:3325)

## Notes and Other Information
- This function is part of PostgreSQL's shared memory initialization sequence
- The calculation must be consistent between calls to ensure proper shared memory allocation
- Uses overflow-safe arithmetic functions to prevent integer overflow in size calculations
- The memory layout consists of the fixed control structure followed by the variable-sized worker array
- Located in src/backend/postmaster/autovacuum.c:3300-3318