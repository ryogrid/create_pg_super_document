# stream_abort_internal

## Location
[src/backend/replication/logical/worker.c:1731-1813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1731-L1813)

## Overview
Handles the internal abort processing for serialized streaming transactions, either cleaning up files for top-level transaction aborts or truncating serialized data for subtransaction rollbacks.

## Definition

```c
static void
stream_abort_internal(TransactionId xid, TransactionId subxid)
```
## Detailed Description
This function manages the abort processing for streaming transactions that have been serialized to files. It handles two distinct scenarios:

1. **Top-level Transaction Abort**: When xid equals subxid, it represents a complete transaction abort, so all associated serialized files are cleaned up
2. **Subtransaction Rollback**: When xid differs from subxid, it processes a subtransaction abort by:
   - Reading the subtransaction information for the top-level transaction
   - Locating the specific subtransaction to be aborted
   - Truncating the serialized changes file at the appropriate offset
   - Removing information about later subtransactions
   - Updating the subtransaction metadata

The function uses reverse iteration when scanning subtransactions for efficiency, as recent subtransactions are more likely to be aborted. It handles empty subtransactions gracefully and maintains transaction boundaries properly.

## Parameters / Member Variables
- `xid`: TransactionId of the top-level streaming transaction
- `subxid`: TransactionId of the subtransaction being aborted (equals xid for top-level aborts)
## Dependencies
- Functions called/Symbols referenced:
  - [stream_cleanup_files](stream_cleanup_files.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [subxact_info_read](subxact_info_read.md)
  - [cleanup_subxact_info](../c/cleanup_subxact_info.md)
  - [end_replication_step](../e/end_replication_step.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [changes_filename](../c/changes_filename.md)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md)
  - [BufFileTruncateFileSet](../B/BufFileTruncateFileSet.md)
  - [BufFileClose](../B/BufFileClose.md)
  - [subxact_info_write](subxact_info_write.md)
- Called from:
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)

## Notes and Other Information
- Uses reverse iteration through subtransaction array for better performance
- Cannot use binary search due to potentially unsorted subtransaction XIDs
- Handles empty subtransactions by cleaning up metadata without file operations
- Properly maintains replication step boundaries with begin/end_replication_step calls
- Commits transaction commands to ensure metadata changes are persistent
- Part of the logical replication streaming transaction abort protocol
- Critical for maintaining data consistency during transaction rollbacks