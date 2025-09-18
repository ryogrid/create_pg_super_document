# RestoreOutput

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1721 - 1735

## Overview
A private function that restores a previously saved output file handle to an archive, properly closing the current output and switching back to the saved output destination.

## Definition
```c
static void RestoreOutput(ArchiveHandle *AH, CompressFileHandle *savedOutput)
```

## Detailed Description
The `RestoreOutput` function implements the restore half of the save/restore output pattern used throughout the PostgreSQL archiver. It safely transitions from the current output file handle back to a previously saved output destination.

The function performs a two-step operation: first, it properly closes the current output file handle using `EndCompressFileHandle`, ensuring that any buffered data is flushed and resources are released. If the close operation fails, the function terminates the program with a fatal error. Upon successful closure, it restores the previously saved output handle to the archive's OF field.

This mechanism is essential for scenarios where archive output needs to be temporarily redirected (such as generating summaries or handling different output streams) and then restored to the original destination. The function ensures proper resource management by explicitly closing the temporary output before restoration.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle structure containing the current archive state
- `savedOutput`: Pointer to a previously saved CompressFileHandle that should be restored as the current output

## Dependencies
- Functions called/Symbols referenced:
  - EndCompressFileHandle
  - pg_fatal
- Types referenced:
  - CompressFileHandle
- Called from (representative examples):
  - RestoreArchive
  - PrintTOCSummary

## Notes and Other Information
- This is a private static function internal to the archiver routines
- Must be used with a CompressFileHandle previously obtained from `SaveOutput`
- Clears errno before attempting to close the current output file
- Provides fatal error handling if the current output file cannot be properly closed
- Essential for proper resource management when switching between output destinations
- Located in `src/bin/pg_dump/pg_backup_archiver.c:1721-1735`
- The function assumes that savedOutput is a valid handle that was previously saved
- Should always be called to restore output after temporary redirection to prevent resource leaks