# PVIndStats

## Location
src/backend/commands/vacuumparallel.c: 136 - 156

## Overview
PVIndStats is a structure that tracks index vacuum statistics and processing status for individual indexes during parallel vacuum operations in PostgreSQL.

## Definition


## Detailed Description
PVIndStats serves as a per-index tracking structure within PostgreSQL's parallel vacuum system. Each index being processed during a parallel vacuum operation has an associated PVIndStats structure that maintains the current processing status, determines whether parallel workers can safely process the index, and stores the results of vacuum operations. The structure coordinates between the leader process and worker processes to ensure proper index processing order and result collection.

## Parameters / Member Variables
- : Current processing status of the index using PVIndVacStatus enum values (INITIAL, NEED_BULKDELETE, NEED_CLEANUP, COMPLETED)
- : Boolean flag indicating whether both leader and workers can process this index (true) or only the leader can handle it (false)
- : Boolean flag indicating whether the index statistics have been updated after processing
- : IndexBulkDeleteResult structure containing the actual statistics and results from index vacuum or cleanup operations

## Dependencies
- Functions called/Symbols referenced:
  - PVIndVacStatus (enum for processing status)
  - IndexBulkDeleteResult (structure for vacuum results)
- Called from (representative examples):
  - ParallelVacuumState (as member array)
  - parallel_vacuum_init
  - parallel_vacuum_process_all_indexes
  - parallel_vacuum_process_safe_indexes
  - parallel_vacuum_process_unsafe_indexes
  - parallel_vacuum_process_one_index
  - parallel_vacuum_main
  - parallel_vacuum_end

## Notes and Other Information
- The status and parallel_workers_can_process fields are set by the leader process before executing parallel index operations
- These control fields are not fixed for the entire VACUUM operation but are set individually for each parallel index vacuum and cleanup phase
- Individual workers or the leader store their processing results in the istat field
- The structure enables coordination between leader and workers to handle both safe indexes (that can be processed in parallel) and unsafe indexes (that require sequential processing by the leader only)
- Part of the broader parallel vacuum infrastructure that includes PVShared and ParallelVacuumState structures