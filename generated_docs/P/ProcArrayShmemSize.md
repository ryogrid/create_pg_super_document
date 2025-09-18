# ProcArrayShmemSize

## Location
[src/backend/storage/ipc/procarray.c:376-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L376-L380)

## Overview
ProcArrayShmemSize calculates and returns the amount of shared memory space needed for the shared process array structure and related Hot Standby data structures.

## Definition


## Detailed Description
This function computes the total shared memory requirements for the process array subsystem, which is central to PostgreSQL's transaction management and Hot Standby functionality. The calculation includes space for the main ProcArrayStruct and, when Hot Standby is enabled, additional space for the KnownAssignedXids tracking system used during recovery operations.

The function performs size calculations in a safe manner using PostgreSQL's overflow-checking arithmetic functions (add_size, mul_size) to prevent integer overflow issues. The total size depends on configuration parameters like MaxBackends and max_prepared_xacts, as well as the EnableHotStandby setting.

## Parameters / Member Variables
- No input parameters (void function)
- Returns:  - Total shared memory bytes required

## Dependencies
- Functions called/Symbols referenced:
  - offsetof (standard C macro)
  - [add_size](../a/add_size.md) (PostgreSQL safe arithmetic)
  - [mul_size](../m/mul_size.md) (PostgreSQL safe arithmetic)
  - MaxBackends (global configuration variable)
  - max_prepared_xacts (global configuration variable)
  - EnableHotStandby (global configuration variable)
  - [ProcArrayStruct](ProcArrayStruct.md) (main structure type)
  - TransactionId (transaction ID type)
  - PGPROC_MAX_CACHED_SUBXIDS (constant)
- Called from:
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (during shared memory initialization)

## Notes and Other Information
- PROCARRAY_MAXPROCS is defined as (MaxBackends + max_prepared_xacts) to account for all possible processes
- TOTAL_MAX_CACHED_SUBXIDS calculation ensures sufficient space for subtransaction tracking in all processes
- Hot Standby structures (KnownAssignedXids arrays) are allocated even if Hot Standby isn't currently active, as this decision must be made during shared memory setup
- The function uses safe size calculation utilities to prevent arithmetic overflow
- Space allocation includes both TransactionId arrays and corresponding boolean validity arrays for Hot Standby operations
- This is a critical function for PostgreSQL's shared memory subsystem initialization