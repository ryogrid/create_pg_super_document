# RemoveTwoPhaseFile

## Location
[src/backend/access/transam/twophase.c:1708-1726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1708-L1726)

## Overview
RemoveTwoPhaseFile deletes the two-phase commit state file from the filesystem for a specified transaction ID.

## Definition
static void RemoveTwoPhaseFile(TransactionId xid, bool giveWarning)

## Detailed Description
This static function handles the filesystem cleanup of two-phase commit state files after a prepared transaction has been committed or rolled back. It constructs the appropriate file path using TwoPhaseFilePath and attempts to remove the file using the standard unlink() system call. The function provides configurable error reporting behavior through the giveWarning parameter - when set to false, it silently handles the common case where the file doesn't exist (particularly during WAL replay scenarios). This flexibility is important because during recovery operations, the system may attempt to remove files that were already cleaned up or never existed on disk.

## Parameters / Member Variables
- `xid`: The transaction ID whose corresponding two-phase commit file should be removed
- `giveWarning`: Boolean flag controlling whether to report warnings for missing files (false suppresses ENOENT warnings, true reports all errors)

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseFilePath](../T/TwoPhaseFilePath.md)
  - unlink
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [PrepareRedoRemove](../P/PrepareRedoRemove.md)

## Notes and Other Information
- This is a static function, only accessible within the twophase.c module
- Designed to handle the common case during WAL replay where files may not exist without generating spurious warnings
- Uses PostgreSQL's standard error reporting mechanism (ereport) with WARNING level for file access errors
- File path construction is delegated to TwoPhaseFilePath for consistency with other file operations
- Critical for preventing accumulation of stale 2PC state files after transaction completion

## Simplified Source

```c
static void RemoveTwoPhaseFile(TransactionId xid, bool giveWarning)
{
    char path[MAXPGPATH];

    TwoPhaseFilePath(path, xid);
    if (unlink(path))
        if (errno != ENOENT || giveWarning)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("could not remove file \"%s\": %m", path)));
}
```