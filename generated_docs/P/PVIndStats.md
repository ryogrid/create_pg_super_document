# PVIndStats

## Location
[src/backend/commands/vacuumparallel.c:136-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L136-L156)

## Overview
PVIndStats is a structure that tracks index vacuum statistics and processing status for individual indexes during parallel vacuum operations in PostgreSQL.

## Definition

```c
typedef struct PVIndStats
{
	/*
	 * The following two fields are set by leader process before executing
	 * parallel index vacuum or parallel index cleanup.  These fields are not
	 * fixed for the entire VACUUM operation.  They are only fixed for an
	 * individual parallel index vacuum and cleanup.
	 *
	 * parallel_workers_can_process is true if both leader and worker can
	 * process the index, otherwise only leader can process it.
	 */
	PVIndVacStatus status;
	bool		parallel_workers_can_process;

	/*
	 * Individual worker or leader stores the result of index vacuum or
	 * cleanup.
	 */
	bool		istat_updated;	/* are the stats updated? */
	IndexBulkDeleteResult istat;
} PVIndStats;
```
## Detailed Description
PVIndStats serves as a per-index tracking structure within PostgreSQL's parallel vacuum system. Each index being processed during a parallel vacuum operation has an associated PVIndStats structure that maintains the current processing status, determines whether parallel workers can safely process the index, and stores the results of vacuum operations. The structure coordinates between the leader process and worker processes to ensure proper index processing order and result collection.

## Parameters / Member Variables
- `status`: Current processing status of the index using PVIndVacStatus enum values (INITIAL, NEED_BULKDELETE, NEED_CLEANUP, COMPLETED)
- `parallel_workers_can_process`: Boolean flag indicating whether both leader and workers can process this index (true) or only the leader can handle it (false)
- `istat_updated`: Boolean flag indicating whether the index statistics have been updated after processing
- `istat`: IndexBulkDeleteResult structure containing the actual statistics and results from index vacuum or cleanup operations
## Dependencies
- Functions called/Symbols referenced:
  - PVIndVacStatus (enum for processing status)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md) (structure for vacuum results)
- Called from (representative examples):
  - [ParallelVacuumState](ParallelVacuumState.md) (as member array)
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md)
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md)
  - [parallel_vacuum_process_safe_indexes](../p/parallel_vacuum_process_safe_indexes.md)
  - [parallel_vacuum_process_unsafe_indexes](../p/parallel_vacuum_process_unsafe_indexes.md)
  - [parallel_vacuum_process_one_index](../p/parallel_vacuum_process_one_index.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)
  - [parallel_vacuum_end](../p/parallel_vacuum_end.md)

## Notes and Other Information
- The status and parallel_workers_can_process fields are set by the leader process before executing parallel index operations
- These control fields are not fixed for the entire VACUUM operation but are set individually for each parallel index vacuum and cleanup phase
- Individual workers or the leader store their processing results in the istat field
- The structure enables coordination between leader and workers to handle both safe indexes (that can be processed in parallel) and unsafe indexes (that require sequential processing by the leader only)
- Part of the broader parallel vacuum infrastructure that includes PVShared and ParallelVacuumState structures