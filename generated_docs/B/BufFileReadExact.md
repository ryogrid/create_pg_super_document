# BufFileReadExact

## Location
[src/backend/storage/file/buffile.c:654-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L654-L663)

## Overview
Reads exactly the specified number of bytes from a buffered file, raising an error if the requested amount cannot be read completely.

## Definition

```c
void
BufFileReadExact(BufFile *file, void *ptr, size_t size)
```
## Detailed Description
BufFileReadExact is a wrapper around BufFileReadCommon that enforces strict reading requirements. It reads exactly the specified number of bytes from the buffered file into the provided buffer. Unlike other read functions that may return fewer bytes than requested, this function guarantees that either all requested bytes are read or an error is raised.

The function calls BufFileReadCommon with the 'exact' parameter set to true and 'eofOK' set to false, meaning:
- It must read exactly 'size' bytes (no short reads allowed)
- Reading zero bytes due to EOF is considered an error

This function is typically used when the caller expects a specific amount of data and partial reads would indicate a corrupted file or unexpected end-of-file condition.

## Parameters / Member Variables
- : Pointer to the BufFile structure representing the buffered file to read from
- : Pointer to the buffer where the read data will be stored
- : Number of bytes that must be read exactly

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileReadCommon](BufFileReadCommon.md) (internal function that performs the actual reading)
- Called from (representative examples):
  - [ReadTempFileBlock](../R/ReadTempFileBlock.md) (GIST index building)
  - [SendBackupManifest](../S/SendBackupManifest.md) (backup operations)
  - [ExecHashJoinGetSavedTuple](../E/ExecHashJoinGetSavedTuple.md) (hash join execution)
  - [apply_spooled_messages](../a/apply_spooled_messages.md) (logical replication)
  - [subxact_info_read](../s/subxact_info_read.md) (logical replication subtransaction handling)
  - [ltsReadBlock](../l/ltsReadBlock.md) (log tape sorting)
  - [sts_read_tuple](../s/sts_read_tuple.md) (shared tuplestore operations)
  - [readtup_heap](../r/readtup_heap.md) (tuplestore heap reading)

## Notes and Other Information
- This function will raise an ERROR (via ereport) if it cannot read exactly the requested number of bytes
- The function automatically flushes any pending writes before attempting to read
- It handles buffering internally, reading from the file's internal buffer when possible and loading more data as needed
- Unlike BufFileReadMaybeEOF, this function does not tolerate EOF conditions - any EOF encountered before reading the full amount is treated as an error
- Used in contexts where data integrity is critical and partial reads indicate corruption or protocol violations