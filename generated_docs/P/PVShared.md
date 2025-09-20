# PVShared

## Location
[src/backend/commands/vacuumparallel.c:57-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L57-L121)

## Overview
PVShared is a structure that holds shared information among parallel workers in PostgreSQL's parallel vacuum operation, allocated in the DSM (Dynamic Shared Memory) segment.

## Definition

```c
typedef struct PVShared
{
	/*
	 * Target table relid and log level (for messages about parallel workers
	 * launched during VACUUM VERBOSE).  These fields are not modified during
	 * the parallel vacuum.
	 */
	Oid			relid;
	int			elevel;

	/*
	 * Fields for both index vacuum and cleanup.
	 *
	 * reltuples is the total number of input heap tuples.  We set either old
	 * live tuples in the index vacuum case or the new live tuples in the
	 * index cleanup case.
	 *
	 * estimated_count is true if reltuples is an estimated value.  (Note that
	 * reltuples could be -1 in this case, indicating we have no idea.)
	 */
	double		reltuples;
	bool		estimated_count;

	/*
	 * In single process vacuum we could consume more memory during index
	 * vacuuming or cleanup apart from the memory for heap scanning.  In
	 * parallel vacuum, since individual vacuum workers can consume memory
	 * equal to maintenance_work_mem, the new maintenance_work_mem for each
	 * worker is set such that the parallel operation doesn't consume more
	 * memory than single process vacuum.
	 */
	int			maintenance_work_mem_worker;

	/*
	 * The number of buffers each worker's Buffer Access Strategy ring should
	 * contain.
	 */
	int			ring_nbuffers;

	/*
	 * Shared vacuum cost balance.  During parallel vacuum,
	 * VacuumSharedCostBalance points to this value and it accumulates the
	 * balance of each parallel vacuum worker.
	 */
	pg_atomic_uint32 cost_balance;

	/*
	 * Number of active parallel workers.  This is used for computing the
	 * minimum threshold of the vacuum cost balance before a worker sleeps for
	 * cost-based delay.
	 */
	pg_atomic_uint32 active_nworkers;

	/* Counter for vacuuming and cleanup */
	pg_atomic_uint32 idx;

	/* DSA handle where the TidStore lives */
	dsa_handle	dead_items_dsa_handle;

	/* DSA pointer to the shared TidStore */
	dsa_pointer dead_items_handle;

	/* Statistics of shared dead items */
	VacDeadItemsInfo dead_items_info;
} PVShared;
```
## Detailed Description
PVShared serves as the central coordination structure for parallel vacuum operations in PostgreSQL. It contains shared state that all parallel workers need to coordinate their activities, including resource management, progress tracking, and dead tuple information sharing. The structure is designed to be allocated in Dynamic Shared Memory (DSM) so that all worker processes can access and modify the shared state safely using atomic operations where necessary.

## Parameters / Member Variables
- : Target table OID that is being vacuumed (not modified during parallel vacuum)
- : Log level for messages about parallel workers launched during VACUUM VERBOSE (not modified during parallel vacuum)
- : Total number of input heap tuples (either old live tuples for index vacuum or new live tuples for index cleanup)
- : True if reltuples is an estimated value (reltuples could be -1 indicating unknown)
- : Memory limit per worker to ensure parallel operation doesn't exceed single-process vacuum memory usage
- : Number of buffers each worker's Buffer Access Strategy ring should contain
- : Shared vacuum cost balance using atomic operations, accumulates balance from all parallel workers
- : Number of active parallel workers, used for computing minimum threshold for cost-based delay
- : Atomic counter for vacuuming and cleanup coordination
- : DSA handle where the TidStore for dead items lives
- : DSA pointer to the shared TidStore containing dead tuple identifiers
- : Statistics about the shared dead items collection

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md)
  - dsa_handle
  - dsa_pointer
  - VacDeadItemsInfo
- Called from (representative examples):
  - [ParallelVacuumState](ParallelVacuumState.md) (as a member)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)

## Notes and Other Information
- The structure is specifically designed for DSM allocation and inter-process sharing
- Uses atomic operations (pg_atomic_uint32) for safe concurrent access to shared counters
- Memory management is carefully designed to prevent parallel workers from consuming more memory than a single-process vacuum would use
- The cost_balance mechanism implements PostgreSQL's vacuum cost-based delay system in a parallel context
- Dead items are managed through a shared TidStore accessible via DSA (Dynamic Shared Area) pointers