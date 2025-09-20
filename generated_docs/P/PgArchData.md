# PgArchData

## Location
[src/backend/postmaster/pgarch.c:84-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L84-L92)

## Overview
PgArchData is a shared memory structure that stores essential data for the PostgreSQL archiver process, including the process number and synchronization flags for directory scanning.

## Definition

```c
typedef struct PgArchData
{
	int			pgprocno;		/* proc number of archiver process */

	/*
	 * Forces a directory scan in pgarch_readyXlog().
	 */
	pg_atomic_uint32 force_dir_scan;
} PgArchData;
```
## Detailed Description
PgArchData is a shared memory structure located in src/backend/postmaster/pgarch.c that serves as the communication interface between the postmaster and the archiver process. This structure contains critical information needed to identify and coordinate with the archiver process across different PostgreSQL processes. The structure is designed to be lightweight yet essential for proper archiver functionality, particularly for tracking the archiver process and signaling when directory rescans are needed for WAL file archiving.

## Parameters / Member Variables
- `pgprocno`: Process number that uniquely identifies the archiver process within the PostgreSQL process array
- `force_dir_scan`: Atomic flag that forces a directory scan in pgarch_readyXlog() when set, ensuring that new WAL files ready for archiving are discovered promptly
## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md)
- Called from (representative examples):
  - [PgArchShmemSize](PgArchShmemSize.md)
  - [PgArchShmemInit](PgArchShmemInit.md)

## Notes and Other Information
- This structure resides in shared memory to enable communication between the postmaster and archiver processes
- The force_dir_scan field uses atomic operations to ensure thread-safe access across process boundaries
- The structure is minimal by design to reduce shared memory overhead while providing essential archiver coordination functionality
- Located at src/backend/postmaster/pgarch.c:84-92