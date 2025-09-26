# PMSignalShmemSize

## Location
[src/backend/storage/ipc/pmsignal.c:129-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L129-L143)

## Overview
Calculates the amount of shared memory space needed for the postmaster signaling system's data structures.

## Definition

```c
Size
PMSignalShmemSize(void)
```
## Detailed Description
PMSignalShmemSize computes the total shared memory space required for the pmsignal.c module's shared memory structures. The calculation includes space for the base PMSignalData structure plus a dynamically-sized array of PMChildFlags elements. The size of this array is determined by MaxLivePostmasterChildren(), ensuring sufficient space for tracking signals to all possible child processes.

The function uses PostgreSQL's safe arithmetic functions (add_size, mul_size) to prevent integer overflow during size calculations, which is crucial for memory allocation safety.

## Parameters / Member Variables
- Returns: Size - the calculated shared memory size in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [PMSignalData](PMSignalData.md) (structure type)
  - [MaxLivePostmasterChildren](../M/MaxLivePostmasterChildren.md) (function to get max child count)
  - [add_size](../a/add_size.md) (safe addition function)
  - [mul_size](../m/mul_size.md) (safe multiplication function)
  - offsetof (standard C macro)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (for total shared memory calculation)
  - [PMSignalShmemInit](PMSignalShmemInit.md) (for memory allocation verification)

## Notes and Other Information
- This is a public function (no static qualifier) used during PostgreSQL startup
- The calculation accounts for variable array size based on configuration
- Uses safe arithmetic to prevent overflow vulnerabilities
- Part of PostgreSQL's shared memory subsystem initialization
- The PMChildFlags array size depends on the maximum number of backend processes configured