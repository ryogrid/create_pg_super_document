# ShmemBackendArraySize

## Location
[src/backend/postmaster/postmaster.c:4550-4555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4550-L4555)

## Overview
Calculates the required shared memory size for the Backend array used in EXEC_BACKEND configurations to track all postmaster child processes.

## Definition
```c
Size ShmemBackendArraySize(void)
```

## Detailed Description
This function computes the amount of shared memory needed to store an array of Backend structures for all possible postmaster child processes. It multiplies the maximum number of live postmaster children by the size of a single Backend structure.

The function is specifically used in EXEC_BACKEND builds (primarily Windows) where the postmaster and its children run in separate processes and need to share backend information through shared memory rather than inherited process memory.

The calculation uses mul_size() for safe multiplication that prevents integer overflow, which is important for memory allocation calculations that could be security-sensitive.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [MaxLivePostmasterChildren](../M/MaxLivePostmasterChildren.md) (returns maximum concurrent child processes)
  - [mul_size](../m/mul_size.md) (safe multiplication function)
  - [Backend](../B/Backend.md) (structure type for size calculation)
- Called from (representative examples):
  - [ShmemBackendArrayAllocation](ShmemBackendArrayAllocation.md)
  - [CalculateShmemSize](../C/CalculateShmemSize.md)
  - POSTMASTER_FD_OWN (referenced in header)

## Notes and Other Information
- Returns Size type representing bytes needed for the Backend array
- Only relevant in EXEC_BACKEND configurations (Windows and some Unix variants)
- Critical for proper shared memory sizing during PostgreSQL startup
- Uses safe arithmetic to prevent overflow in memory size calculations
- Part of the broader shared memory initialization and management system
- The returned size is used by shared memory allocation routines during startup

## Simplified Source

```c
// Simplified version of ShmemBackendArraySize
Size ShmemBackendArraySize(void) {
    // Calculate total memory needed for Backend array
    // = (max number of child processes) × (size of one Backend struct)
    return mul_size(MaxLivePostmasterChildren(), sizeof(Backend));
}
```

Key simplifications made:
- Added explanatory comments for the calculation logic
- Function is already very simple, so main improvement is clarity through comments
- Preserved the safe multiplication using mul_size() which prevents overflow