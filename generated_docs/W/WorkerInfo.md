# WorkerInfo

## Location
[src/bin/pg_dump/parallel.c:127-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L127-L130)

## Overview
A typedef pointer to WorkerInfoData structure that holds information about a single autovacuum worker's state and whereabouts in PostgreSQL's autovacuum system.

## Definition


## Detailed Description
WorkerInfo is a pointer type to WorkerInfoData structure that tracks individual autovacuum worker processes in PostgreSQL. The system maintains an array of these structures in shared memory, sized according to the autovacuum_max_workers configuration parameter. Each structure contains essential information about a worker's current state, including which database and table it's working on, process information, and coordination flags for load balancing.

## Parameters / Member Variables
- : Entry into free list or running list for worker management
- : OID of the database this worker is assigned to work on
- : OID of the table currently being vacuumed (if any)
- : Pointer to PGPROC of the running worker process, NULL if not started
- : Timestamp when this worker was launched
- : Atomic flag indicating whether this worker should be included in load balance calculations
- : Boolean flag indicating whether the current table is marked as relisshared

## Dependencies
- Functions called/Symbols referenced:
  - [WorkerInfoData](WorkerInfoData.md) (underlying structure)
  - [dlist_node](../d/dlist_node.md) (for linked list management)
  - [PGPROC](../P/PGPROC.md) (process information)
  - TimestampTz (timestamp type)
  - [pg_atomic_flag](../p/pg_atomic_flag.md) (atomic operations)
- Called from (representative examples):
  - [AutoVacuumShmemStruct](../A/AutoVacuumShmemStruct.md) (as array member)
  - [do_start_worker](../d/do_start_worker.md)
  - [autovac_recalculate_workers_for_balance](../a/autovac_recalculate_workers_for_balance.md)
  - [do_autovacuum](../d/do_autovacuum.md)
  - [AutoVacuumShmemInit](../A/AutoVacuumShmemInit.md)

## Notes and Other Information
- All fields are protected by AutovacuumLock, except wi_tableoid and wi_sharedrel which are protected by AutovacuumScheduleLock
- The wi_tableoid and wi_sharedrel fields are read-only for all processes except the worker itself
- Part of PostgreSQL's autovacuum infrastructure located in src/backend/postmaster/autovacuum.c
- Used for coordinating and tracking multiple autovacuum worker processes in a shared memory environment
- Essential for load balancing and resource management in the autovacuum system