# pg_safe_snapshot_blocking_pids

## Location
src/backend/utils/adt/lockfuncs.c: 573 - 612

## Overview
pg_safe_snapshot_blocking_pids identifies processes that are preventing a given PID from obtaining a safe snapshot for serializable transactions.

## Definition
Datum pg_safe_snapshot_blocking_pids(PG_FUNCTION_ARGS)

## Detailed Description
pg_safe_snapshot_blocking_pids is specifically designed to support PostgreSQL's serializable isolation level by identifying which processes are preventing a given process from obtaining a safe snapshot. A safe snapshot is one where the process can begin a serializable transaction without risk of serialization anomalies. This function works by calling GetSafeSnapshotBlockingPids to collect information about processes that must complete their transactions before the specified process can safely start its serializable transaction. The function allocates a buffer large enough for the maximum possible number of blocking processes (MaxBackends) and converts the resulting integer array into a PostgreSQL array datum for return to the caller.

## Parameters / Member Variables
- `blocked_pid`: INT32 - The process ID of the process that is waiting for a safe snapshot

The function returns an array of INT32 values representing the PIDs of processes that must complete before the specified process can obtain a safe snapshot.

## Dependencies
- Functions called/Symbols referenced:
  - GetSafeSnapshotBlockingPids (core function that identifies blocking processes)
  - [construct_array_builtin](../c/construct_array_builtin.md) (constructs PostgreSQL array result)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - [Int32GetDatum](../I/Int32GetDatum.md) (converts integers to PostgreSQL datums)
- Referenced constants:
  - MaxBackends (maximum number of backend processes)
- Called from:
  - SQL queries and applications monitoring serializable transaction conflicts
  - Deadlock detection and analysis tools
  - Performance monitoring systems tracking serialization delays

## Notes and Other Information
- This function is specifically related to PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The function does not currently consider parallel-query cases, which may be a limitation in some scenarios
- The buffer is allocated to handle the maximum possible number of blockers (MaxBackends) to avoid truncation
- Unlike regular lock blocking, safe snapshot blocking is specifically about transaction ordering for serializable isolation
- The function returns an empty array if no processes are blocking safe snapshot acquisition
- This functionality is crucial for understanding and debugging serializable transaction delays
- The blocking relationships identified here are different from regular lock conflicts - they represent transaction ordering constraints for maintaining serializability
- Safe snapshots are required to ensure that serializable transactions can detect conflicts and maintain the serializable isolation guarantee