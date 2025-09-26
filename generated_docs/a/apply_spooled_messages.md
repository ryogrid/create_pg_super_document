# apply_spooled_messages

## Location
[src/backend/replication/logical/worker.c:2003-2132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2003-L2132)

## Overview
apply_spooled_messages processes spooled replication messages from a file for a committed or prepared streaming transaction in PostgreSQL logical replication, replaying all changes sequentially.

## Definition
```c
void apply_spooled_messages(FileSet *stream_fileset, TransactionId xid, XLogRecPtr lsn)
```

## Detailed Description
This function is the core mechanism for processing spooled replication messages that were temporarily stored in files during large streaming transactions. It opens the changes file associated with the given transaction ID, reads messages sequentially, and replays each one using the apply_dispatch mechanism. The function handles memory management carefully by allocating buffers in TopTransactionContext to avoid resets between message processing, and switches to ApplyMessageContext for individual message processing.

The function tracks progress with debug logging and ensures proper cleanup by validating that the file ends at the expected position when stream_fd is closed during processing (typically after a stream_commit message). It operates within replication steps and maintains resource ownership properly to prevent accidental file closures during subtransaction aborts.

## Parameters / Member Variables
- `stream_fileset`: FileSet containing the streaming transaction changes files
- `xid`: Transaction ID identifying the specific changes file to process
- `lsn`: LSN of the commit/prepare record, used for skipping changes and as remote_final_lsn

## Dependencies
- Functions called/Symbols referenced:
  - [am_parallel_apply_worker](am_parallel_apply_worker.md)
  - [maybe_start_skipping_changes](../m/maybe_start_skipping_changes.md)
  - [begin_replication_step](../b/begin_replication_step.md)/end_replication_step
  - [changes_filename](../c/changes_filename.md)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md)/BufFileReadMaybeEOF/BufFileReadExact/BufFileTell
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [apply_dispatch](apply_dispatch.md)
  - [ensure_last_message](../e/ensure_last_message.md)
  - [stream_close_file](../s/stream_close_file.md)
  - Memory management functions (palloc, repalloc, MemoryContextSwitchTo, MemoryContextReset)
- Called from (representative examples):
  - [apply_handle_stream_commit](apply_handle_stream_commit.md)
  - [apply_handle_stream_prepare](apply_handle_stream_prepare.md)  
  - [pa_process_spooled_messages_if_required](../p/pa_process_spooled_messages_if_required.md) (parallel apply worker)

## Notes and Other Information
- Sets in_remote_transaction = true and remote_final_lsn for proper apply_dispatch context
- Uses BLCKSZ as initial buffer size, dynamically reallocating as needed for larger messages
- Includes progress logging every 1000 changes for debugging large transactions
- Handles both normal completion (reading entire file) and early termination (when stream_fd is closed by stream_commit processing)
- Critical for handling large streaming transactions that exceed memory limits and require spooling to disk
- Resource ownership is temporarily transferred to TopTransactionResourceOwner to prevent subtransaction interference