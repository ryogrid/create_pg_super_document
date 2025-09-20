# WorkerInfoData

## Location
[src/backend/postmaster/autovacuum.c:227-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L227-L236)

## Overview
The  structure tracks the status and activity of individual autovacuum worker processes in shared memory, enabling coordination and management of concurrent autovacuum operations across the PostgreSQL cluster.

## Definition

```c
typedef struct WorkerInfoData
{
	dlist_node	wi_links;
	Oid			wi_dboid;
	Oid			wi_tableoid;
	PGPROC	   *wi_proc;
	TimestampTz wi_launchtime;
	pg_atomic_flag wi_dobalance;
	bool		wi_sharedrel;
} WorkerInfoData;
```
## Detailed Description
The  structure serves as a shared memory descriptor for tracking individual autovacuum worker processes. It maintains essential information about each worker's current assignment, execution state, and coordination requirements. This structure is crucial for the autovacuum launcher's ability to monitor running workers, manage resource allocation, and coordinate vacuum cost balancing across multiple concurrent operations. The structure is designed with thread-safety considerations, using different locks to protect different field groups based on their access patterns.

## Parameters / Member Variables
- : Doubly-linked list node for organizing workers into free or running lists
- : OID of the database that this worker is assigned to process
- : OID of the specific table currently being vacuumed (if any)
- : Pointer to the PGPROC structure of the running worker process (NULL if not started)
- : Timestamp indicating when this worker was launched
- : Atomic flag indicating whether this worker should participate in vacuum cost balance calculations
- : Boolean flag indicating whether the current table is a shared relation

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](../d/dlist_node.md) (doubly-linked list operations)
  - [PGPROC](../P/PGPROC.md) (process control structure)
  - [pg_atomic_flag](../p/pg_atomic_flag.md) (atomic boolean operations)
- Called from (representative examples):
  - [WorkerInfo](WorkerInfo.md) (type alias)
  - [do_start_worker](../d/do_start_worker.md)
  - [autovac_recalculate_workers_for_balance](../a/autovac_recalculate_workers_for_balance.md)
  - [do_autovacuum](../d/do_autovacuum.md)
  - [AutoVacuumShmemSize](../A/AutoVacuumShmemSize.md)

## Notes and Other Information
- Stored in shared memory with array size determined by autovacuum_max_workers configuration
- Field access is protected by different locks: AutovacuumLock for most fields, AutovacuumScheduleLock for wi_tableoid and wi_sharedrel
- The wi_tableoid and wi_sharedrel fields are read-only for all processes except the worker itself
- Critical for implementing vacuum cost balancing across concurrent workers
- Enables the launcher to monitor worker progress and detect hung or failed workers
- The atomic flag for wi_dobalance ensures thread-safe balance calculation participation