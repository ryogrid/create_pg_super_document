# PgArchShmemSize

## Location
[src/backend/postmaster/pgarch.c:157-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L157-L167)

## Overview
PgArchShmemSize calculates and returns the amount of shared memory space required for the PostgreSQL archiver subsystem.

## Definition
```c
Size PgArchShmemSize(void)
```

## Detailed Description
This function computes the total shared memory size needed by the PostgreSQL archiver subsystem. It specifically calculates the memory required for the PgArchData structure, which contains the shared state information for the archiver process. The function is part of PostgreSQL's shared memory initialization infrastructure and is called during server startup to determine memory allocation requirements.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md): Safely adds sizes together with overflow checking
  - [PgArchData](PgArchData.md): Structure containing archiver shared memory data
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md): Main shared memory size calculation function
  - [PgArchShmemInit](PgArchShmemInit.md): Archiver shared memory initialization function

## Notes and Other Information
- Returns a Size type value representing the number of bytes needed
- Part of the PostgreSQL shared memory subsystem initialization sequence
- Must be called before PgArchShmemInit during server startup
- The function uses add_size() for safe arithmetic to prevent integer overflow
- Currently only accounts for the PgArchData structure size