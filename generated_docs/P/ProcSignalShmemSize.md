# ProcSignalShmemSize

## Location
[src/backend/storage/ipc/procsignal.c:111-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L111-L124)

## Overview
Computes the shared memory space needed for PostgreSQL's process signaling system, calculating the total size required for process signal slots and header structure.

## Definition

```c
Size
ProcSignalShmemSize(void)
```
## Detailed Description
ProcSignalShmemSize is a memory calculation function that determines the amount of shared memory required for the process signaling infrastructure. It calculates the space needed for an array of ProcSignalSlot structures plus the ProcSignalHeader structure. The function performs safe arithmetic operations using mul_size and add_size to avoid integer overflow when computing memory requirements for large numbers of process slots.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md)
  - [add_size](../a/add_size.md)
  - NumProcSignalSlots (variable)
  - [ProcSignalSlot](ProcSignalSlot.md) (type)
  - ProcSignalHeader (type)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md)
  - [ProcSignalShmemInit](ProcSignalShmemInit.md)

## Notes and Other Information
- Uses safe arithmetic functions (mul_size, add_size) to prevent integer overflow
- Critical for shared memory allocation during PostgreSQL startup
- The calculation includes space for NumProcSignalSlots signal slots plus the header overhead
- Located in src/backend/storage/ipc/procsignal.c:111-124