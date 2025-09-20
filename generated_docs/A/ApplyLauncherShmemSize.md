# ApplyLauncherShmemSize

## Location
[src/backend/replication/logical/launcher.c:912-930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L912-L930)

## Overview
Computes the amount of shared memory space needed for the PostgreSQL logical replication launcher subsystem.

## Definition
```c
Size ApplyLauncherShmemSize(void)
```

## Detailed Description
This function calculates the total shared memory size required for the logical replication launcher infrastructure. It determines the space needed for both the fixed context structure (LogicalRepCtxStruct) and the dynamic array of LogicalRepWorker structures. The calculation uses proper memory alignment (MAXALIGN) and safe arithmetic functions (add_size, mul_size) to prevent integer overflow when computing memory requirements. The size calculation is based on the max_logical_replication_workers configuration parameter.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepCtxStruct
  - [add_size](../a/add_size.md)
  - [mul_size](../m/mul_size.md)
  - [LogicalRepWorker](../L/LogicalRepWorker.md)
- Called from (representative examples):
  - [ApplyLauncherShmemInit](ApplyLauncherShmemInit.md)
  - [CalculateShmemSize](../C/CalculateShmemSize.md)
  - LOGICALLAUNCHER_H

## Notes and Other Information
- Used during PostgreSQL startup to determine shared memory allocation requirements
- Ensures proper memory alignment using MAXALIGN macro
- Uses safe arithmetic functions to prevent integer overflow in memory calculations
- The returned size includes space for the main context structure plus an array of worker structures
- Part of PostgreSQL's shared memory subsystem initialization process
- Returns a Size type representing the total bytes needed