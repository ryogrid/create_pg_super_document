# set_apply_error_context_xact

## Location
[src/backend/replication/logical/worker.c:5041-5048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L5041-L5048)

## Overview
Sets transaction-specific information in the global error callback context structure for logical replication error reporting.

## Definition
static inline void set_apply_error_context_xact(TransactionId xid, XLogRecPtr lsn)

## Detailed Description
This function is a simple utility that updates the global apply_error_callback_arg structure with transaction-specific information. It sets two key pieces of information that will be used by the apply_error_callback function when errors occur:

1. The remote transaction ID (remote_xid) from the source database
2. The finish LSN (Log Sequence Number) where the transaction completed

This information is crucial for error reporting in logical replication because it allows administrators to:
- Identify exactly which remote transaction was being processed when an error occurred
- Determine the precise WAL position associated with the problematic transaction
- Correlate errors with specific points in the replication stream

The function is marked as static inline, indicating it's optimized for performance and only used within the same source file. It's typically called at the beginning of transaction processing operations to establish the error context.

## Parameters / Member Variables
- : The TransactionId from the remote database that is currently being processed
- : The XLogRecPtr (LSN) indicating the finish position of the transaction in the WAL

## Dependencies
- Functions called/Symbols referenced:
  - None (direct assignment to global variable)
- Called from (representative examples):
  - [apply_handle_begin](../a/apply_handle_begin.md) (when starting transaction processing)
  - [apply_handle_begin_prepare](../a/apply_handle_begin_prepare.md) (for prepared transactions)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md) (when committing prepared transactions)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md) (when rolling back prepared transactions)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md) (for streaming transaction preparation)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md) (when starting streaming)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md) (when aborting streaming)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md) (when committing streaming)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md) (for context cleanup)

## Notes and Other Information
- This is a static inline function for optimal performance
- Updates the global apply_error_callback_arg structure directly
- Part of the error reporting infrastructure for logical replication
- Called at various points during transaction lifecycle management
- Works in conjunction with apply_error_callback to provide rich error context
- Located in src/backend/replication/logical/worker.c:5041-5048