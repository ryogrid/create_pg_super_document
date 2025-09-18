# TXNEntryFile

## Location
src/backend/replication/logical/reorderbuffer.c: 150 - 155

## Overview
TXNEntryFile is a virtual file descriptor structure that tracks file operations and maintains the current offset position for reading and writing transaction entry data to disk.

## Definition


## Detailed Description
This structure encapsulates a virtual file descriptor along with offset tracking functionality for managing transaction entry files in PostgreSQL's logical replication system. When the reorder buffer needs to spill transaction data to disk (typically when memory usage exceeds configured limits), it uses this structure to manage the file operations. The virtual file descriptor (vfd) provides an abstraction over regular file operations, while the curOffset field tracks the current position within the file for sequential read/write operations. The offset is reset to 0 when the file is reopened, ensuring proper positioning for subsequent operations.

## Parameters / Member Variables
- : Virtual file descriptor (File type) that represents the open file, set to -1 when the file is closed
- : Current file offset (off_t) that tracks the position for the next read or write operation, automatically reset to 0 when the file is opened

## Dependencies
- Functions called/Symbols referenced:
  - File (PostgreSQL virtual file descriptor type)
  - off_t (POSIX file offset type)
- Called from (representative examples):
  - [ReorderBufferIterTXNEntry](../R/ReorderBufferIterTXNEntry.md) (at src/backend/replication/logical/reorderbuffer.c:163)
  - IsInsertOrUpdate (at src/backend/replication/logical/reorderbuffer.c:269)
  - [ReorderBufferRestoreChanges](../R/ReorderBufferRestoreChanges.md) (at src/backend/replication/logical/reorderbuffer.c:4388)

## Notes and Other Information
This structure is used when the reorder buffer needs to spill transaction data to temporary files on disk, typically when processing large transactions that exceed memory limits. The virtual file descriptor approach allows PostgreSQL to manage file resources efficiently, including automatic cleanup and resource limit enforcement. The offset tracking is crucial for maintaining the correct file position during sequential operations without requiring explicit seek operations for every read/write.