# fsync_fname

## Location
[src/common/file_utils.c:378-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L378-L433)

## Overview
A wrapper function that performs filesystem synchronization on a file or directory, handling OS-specific errors appropriately for directories.

## Definition

```c
int
fsync_fname(const char *fname, bool isdir)
```
## Detailed Description
 is a high-level wrapper around  that provides a simplified interface for synchronizing files or directories to persistent storage. The function delegates to  with default parameters, specifically using  for the ignore_nonexistent parameter and  log level via . When synchronizing directories, it gracefully handles errors that indicate the operating system doesn't allow or require directory synchronization, which is common on some filesystems.

## Parameters / Member Variables
- : Path to the file or directory to synchronize
- : Boolean flag indicating whether the target is a directory (true) or file (false)

## Dependencies
- Functions called/Symbols referenced:
  - [fsync_fname_ext](fsync_fname_ext.md)
  - [data_sync_elevel](../d/data_sync_elevel.md)
- Called from (representative examples):
  - [CheckPointLogicalRewriteHeap](../C/CheckPointLogicalRewriteHeap.md)
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
  - [CheckPointTwoPhase](../C/CheckPointTwoPhase.md)
  - [CreateDirAndVersionFile](../C/CreateDirAndVersionFile.md)
  - [SnapBuildSerialize](../S/SnapBuildSerialize.md)
  - [copydir](../c/copydir.md)
  - [sync_pgdata](../s/sync_pgdata.md)
  - [durable_rename](../d/durable_rename.md)

## Notes and Other Information
This function is widely used throughout PostgreSQL for ensuring data durability during critical operations like checkpoints, replication slot management, and database directory operations. It's particularly important for crash recovery guarantees and maintaining ACID properties. The function is part of PostgreSQL's file descriptor management subsystem and provides a consistent interface across different operating systems with varying fsync capabilities.