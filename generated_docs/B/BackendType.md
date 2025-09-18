# BackendType

## Location
[src/include/miscadmin.h:369-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/miscadmin.h#L369-L370)

## Overview
An enumeration that defines the various types of PostgreSQL backend processes and their roles within the system architecture.

## Definition


## Detailed Description
BackendType is a comprehensive enumeration that categorizes all PostgreSQL process types within the system. It serves as a fundamental identifier for process classification and is used throughout the PostgreSQL codebase for process management, statistics tracking, and system administration. The enumeration is organized into logical groups: regular backends, auxiliary processes with PGPROC entries, and special processes like the logger that operate independently of shared memory.

## Parameters / Member Variables
- : Invalid or uninitialized backend type (value 0)
- : Regular user connection backend process
- : Autovacuum launcher daemon process
- : Autovacuum worker process
- : Background worker process
- : WAL sender process for replication
- : Slot synchronization worker process
- : Standalone backend (single-user mode)
- : WAL archiver process
- : Background writer process
- : Checkpoint process
- : Startup/recovery process
- : WAL receiver process for replication
- : WAL summarizer process
- : WAL writer process
- : System logger process

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - [PostmasterChildName](../P/PostmasterChildName.md)
  - [postmaster_child_launch](../p/postmaster_child_launch.md)
  - [SubPostmasterMain](../S/SubPostmasterMain.md)
  - SignalChildren
  - [StartChildProcess](../S/StartChildProcess.md)
  - [pgstat_bktype_io_stats_valid](../p/pgstat_bktype_io_stats_valid.md)
  - [pgstat_tracks_io_bktype](../p/pgstat_tracks_io_bktype.md)
  - [GetBackendTypeDesc](../G/GetBackendTypeDesc.md)

## Notes and Other Information
- The global variable MyBackendType (of type BackendType) indicates the current process type
- BACKEND_NUM_TYPES constant defines the total number of backend types (B_LOGGER + 1)
- When adding new entries, the child_process_kinds array in launch_backend.c must be updated accordingly
- Auxiliary processes have PGPROC entries but cannot run transactions or take heavyweight locks
- The logger process is unique in that it doesn't connect to shared memory or have a PGPROC entry
- This enumeration is critical for PostgreSQL's process architecture and system monitoring