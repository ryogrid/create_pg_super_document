# BufFileTell

## Location
[src/backend/storage/file/buffile.c:833-850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L833-L850)

## Overview
Returns the current file position within a BufFile, providing both the file number and byte offset within that file for multi-file buffered I/O operations.

## Definition
```c
void BufFileTell(BufFile *file, int *fileno, off_t *offset)
```

## Detailed Description
BufFileTell retrieves the current position within a BufFile structure by returning two components: the current file number and the byte offset within that file. This function is essential for tracking position in multi-file buffered I/O scenarios where data may span across multiple underlying files. The position is calculated by combining the current file's offset (curOffset) with the current buffer position (pos).

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure from which to get the current position
- `fileno`: Output parameter - receives the current file number within the BufFile set
- `offset`: Output parameter - receives the byte offset within the current file

## Dependencies
- Functions called/Symbols referenced:
  - [BufFile](BufFile.md) (structure type)
- Called from (representative examples):
  - [ensure_last_message](../e/ensure_last_message.md) (src/backend/replication/logical/worker.c:1988)
  - [apply_spooled_messages](../a/apply_spooled_messages.md) (src/backend/replication/logical/worker.c:2088)
  - [subxact_info_add](../s/subxact_info_add.md) (src/backend/replication/logical/worker.c:4187)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (src/backend/utils/sort/tuplestore.c:500)
  - [tuplestore_puttuple_common](../t/tuplestore_puttuple_common.md) (src/backend/utils/sort/tuplestore.c:847)
  - [tuplestore_gettuple](../t/tuplestore_gettuple.md) (src/backend/utils/sort/tuplestore.c:965)

## Notes and Other Information
- The function is straightforward and performs no validation - it assumes the BufFile pointer is valid
- Used primarily by tuplestore operations and logical replication worker processes
- Essential for implementing seek operations and position tracking in buffered file I/O
- The returned position can be used with BufFileSeek to return to the same location later