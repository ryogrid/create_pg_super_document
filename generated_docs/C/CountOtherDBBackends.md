# CountOtherDBBackends

## Location
src/backend/storage/ipc/procarray.c: 3749 - 3752

## Overview
CountOtherDBBackends checks for other active backends running in a specified database, waiting up to 5 seconds for them to exit, and is primarily used to safely interlock DROP DATABASE operations.

## Definition


## Detailed Description
This function implements a critical safety mechanism for database operations that require exclusive access, particularly DROP DATABASE commands. It scans the process array to identify all backends connected to a specific database (excluding the current backend) and attempts to wait for them to exit gracefully.

The function employs a polling strategy with up to 50 iterations of 100ms sleeps (total 5 seconds maximum wait). During each iteration, it counts active backends and prepared transactions in the target database. For autovacuum processes, it proactively sends SIGTERM signals to encourage early termination, while regular user backends are simply monitored until they exit naturally.

The function serves as a protective barrier against data corruption that could occur if a database were dropped while backends were still actively using it. It's designed to work in conjunction with database-level exclusive locks held by the caller.

## Parameters / Member Variables
- : OID of the database to check for active connections
- : Output parameter - number of active regular backends in the database
- : Output parameter - number of prepared transactions in the database
- Returns:  - true if other backends still exist after timeout, false if database is clear

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (main process array structure)
  - procArray (global process array instance)
  - allProcs (global process table)
  - ProcGlobal (global process state)
  - MyProc (current backend's process entry)
  - LWLockAcquire/LWLockRelease (locking primitives)
  - CHECK_FOR_INTERRUPTS (interrupt handling)
  - kill (system call for sending signals)
  - [pg_usleep](../p/pg_usleep.md) (PostgreSQL sleep function)
- Called from:
  - [createdb](../c/createdb.md) (database creation command)
  - [dropdb](../d/dropdb.md) (database drop command)
  - [RenameDatabase](../R/RenameDatabase.md) (database rename operation)
  - [movedb](../m/movedb.md) (database move operation)

## Notes and Other Information
- MAXAUTOVACPIDS constant limits how many autovacuum processes can be signaled per iteration (10)
- The function specifically targets autovacuum backends with SIGTERM for faster cleanup
- Current backend (MyProc) is always excluded from the count
- Prepared transactions (proc->pid == 0) are counted separately as they cannot be terminated
- Function requires the caller to hold appropriate locks on the target database
- Cannot detect backends that are in the process of connecting to the database
- Times out after 5 seconds even if backends remain, returning true to indicate conflicts
- Used as a safety mechanism to prevent data corruption during database-level operations