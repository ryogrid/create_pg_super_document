# PgArchForceDirScan

## Location
[src/backend/postmaster/pgarch.c:802-815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L802-L815)

## Overview
A function that forces the PostgreSQL archiver process to perform a directory scan on its next iteration, ensuring immediate detection of important files like timeline history files.

## Definition
```c
void PgArchForceDirScan(void)
```

## Detailed Description
`PgArchForceDirScan` is a coordination mechanism between different PostgreSQL processes and the archiver process. When called, it sets an atomic flag that instructs the archiver to perform a full directory scan of the WAL archive_status directory on its next call to `pgarch_readyXlog()`. 

This function is particularly important for ensuring that critical files such as timeline history files are discovered and archived as quickly as possible, rather than waiting for the archivers normal polling cycle. The function uses atomic operations to ensure thread-safe communication between processes.

The forced directory scan bypasses any caching or optimization that the archiver might normally use, ensuring that newly created `.ready` files are immediately visible to the archival process.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_write_membarrier_u32](../p/pg_atomic_write_membarrier_u32.md): Atomically writes a 32-bit value with memory barrier semantics
  - `PgArch->force_dir_scan`: Global archiver state variable accessed atomically

- Called from (representative examples):
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md): Called when important WAL files need immediate archival attention
  - External modules that need to ensure prompt archival of specific files

## Notes and Other Information
- This is a public function (non-static) that can be called from other parts of the PostgreSQL codebase
- The function uses atomic operations to ensure safe cross-process communication
- Memory barrier semantics ensure that the write is visible to other processes immediately
- Primarily used for timeline history files and other critical WAL-related files that require prompt archival
- The actual directory scan occurs in `pgarch_readyXlog()` when it detects the flag is set
- This mechanism helps maintain PostgreSQLs reliability guarantees for point-in-time recovery scenarios