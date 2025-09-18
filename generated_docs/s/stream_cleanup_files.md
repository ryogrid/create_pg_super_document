# stream_cleanup_files

## Location
src/backend/replication/logical/worker.c: 4218 - 4241

## Overview
Removes temporary files containing serialized changes and subxact information for a specific subscription and toplevel transaction in PostgreSQL logical replication streaming.

## Definition
```c
void stream_cleanup_files(Oid subid, TransactionId xid)
```

## Detailed Description
This function performs cleanup operations for files associated with a specific subscription and toplevel transaction during logical replication streaming. It removes both the changes file (containing serialized changes) and the subxact file (containing subtransaction information) that were created during the streaming process. The function uses the logical replication worker's stream fileset to manage these temporary files. The changes file removal is mandatory (will not ignore missing files), while the subxact file removal is optional (ignores if file does not exist).

## Parameters / Member Variables
- `subid`: Subscription ID (Oid) identifying which subscription the files belong to
- `xid`: Transaction ID (TransactionId) identifying the toplevel transaction

## Dependencies
- Functions called/Symbols referenced:
  - changes_filename
  - BufFileDeleteFileSet  
  - subxact_filename
- Called from (representative examples):
  - pa_free_worker_info
  - apply_handle_stream_prepare
  - stream_abort_internal
  - apply_handle_stream_commit

## Notes and Other Information
- This function is called during transaction cleanup, whether the transaction commits, aborts, or is prepared
- The changes file deletion uses `false` for the missing_ok parameter, meaning it expects the file to exist
- The subxact file deletion uses `true` for the missing_ok parameter, since subtransaction files may not exist for all transactions
- Uses MyLogicalRepWorker->stream_fileset to manage the file operations
- Each subscription maintains separate sets of files for different toplevel transactions
- This is part of the cleanup process that ensures no temporary files are left behind after transaction processing