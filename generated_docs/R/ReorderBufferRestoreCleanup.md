# ReorderBufferRestoreCleanup

## Location
[src/backend/replication/logical/reorderbuffer.c:4698-4727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4698-L4727)

## Overview
ReorderBufferRestoreCleanup removes all on-disk serialized files for a given transaction by iterating through WAL segments and deleting the associated spill files.

## Definition
```c
static void ReorderBufferRestoreCleanup(ReorderBuffer *rb, ReorderBufferTXN *txn)
```

## Detailed Description
This function is responsible for cleaning up all disk-based storage used by a transaction when it is no longer needed. It calculates the range of WAL segments that might contain serialized changes for the transaction and systematically removes all associated spill files. The function uses the transactions first and final LSN to determine the segment range, then generates the appropriate file paths and attempts to delete them.

The cleanup process handles the case where files might not exist (ENOENT error is ignored) but reports errors for other file system issues. This function is typically called when a transaction is being cleaned up or truncated, ensuring that disk space is reclaimed.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer (used for context, though not heavily used in this function)
- `txn`: Pointer to the ReorderBufferTXN transaction whose spill files should be removed

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg (WAL segment calculation)
  - [ReorderBufferSerializedPath](ReorderBufferSerializedPath.md) (file path generation)
  - unlink (file deletion system call)
  - ereport/ERROR (error reporting)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (error code generation)
- Called from (representative examples):
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
  - [ReorderBufferTruncateTXN](ReorderBufferTruncateTXN.md)

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function requires that the transaction has valid first_lsn and final_lsn values
- File deletion errors are ignored if the file doesnt exist (ENOENT), but other errors are reported
- The function iterates through all possible WAL segments between first and final LSN to ensure complete cleanup
- Critical for preventing disk space leaks in the logical replication spill-to-disk mechanism
- The function uses the current replication slot context (MyReplicationSlot) for file path generation