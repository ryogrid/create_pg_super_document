# process_syncing_tables

## Location
src/backend/replication/logical/tablesync.c: 693 - 723

## Overview
Handles logical replication table synchronization state changes by dispatching to appropriate worker type-specific functions based on the current worker type.

## Definition


## Detailed Description
The  function serves as a central dispatcher for managing table synchronization operations in PostgreSQL's logical replication system. It examines the worker type of the current logical replication worker and delegates the actual synchronization processing to specialized functions based on the worker type.

The function uses a switch statement to handle three different worker types:
- WORKERTYPE_PARALLEL_APPLY: Skips processing as parallel apply workers only operate on tables already in READY state
- WORKERTYPE_TABLESYNC: Delegates to  for table synchronization workers
- WORKERTYPE_APPLY: Delegates to  for apply workers managing multiple table synchronizations
- WORKERTYPE_UNKNOWN: Throws an error as this should never occur

This design allows different worker types to have specialized synchronization logic while providing a unified interface.

## Parameters / Member Variables
- : XLogRecPtr representing the current log sequence number (LSN) in the WAL stream, used to determine synchronization progress and state transitions

## Dependencies
- Functions called/Symbols referenced:
  - [process_syncing_tables_for_sync](process_syncing_tables_for_sync.md)
  - process_syncing_tables_for_apply
  - MyLogicalRepWorker (global variable)
  - WORKERTYPE_PARALLEL_APPLY, WORKERTYPE_TABLESYNC, WORKERTYPE_APPLY, WORKERTYPE_UNKNOWN (enum values)
  - elog (for error reporting)

- Called from (representative examples):
  - [apply_handle_commit](../a/apply_handle_commit.md)
  - [apply_handle_prepare](../a/apply_handle_prepare.md)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:693-723
- This function is called at transaction boundaries and streaming checkpoints to ensure table synchronization states are properly managed
- The parallel apply worker case is explicitly skipped because parallel workers only handle tables that are already synchronized (READY state)
- Error handling ensures that unknown worker types are caught early with a clear error message
- The function is part of PostgreSQL's logical replication infrastructure that enables selective table synchronization between publisher and subscriber nodes