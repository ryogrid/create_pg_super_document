# CleanupTempFiles

## Location
[src/backend/storage/file/fd.c:3199-3270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3199-L3270)

## Overview
The core function responsible for closing temporary files and deleting their underlying files, with different behavior depending on whether it's called during transaction end or process exit.

## Definition
```c
static void CleanupTempFiles(bool isCommit, bool isProcExit)
```

## Detailed Description
This function performs comprehensive cleanup of temporary files and allocated file descriptors. It operates in two main modes:

**Transaction-level cleanup (isProcExit = false)**:
- Only closes files marked with FD_CLOSE_AT_EOXACT (transaction-local temporary files)
- Issues warnings for any files that should have been closed by ResourceOwner but weren't
- Used during normal transaction commit/abort

**Process exit cleanup (isProcExit = true)**:
- Closes ALL temporary files, including those marked with FD_DELETE_AT_CLOSE
- No warnings issued as this is expected final cleanup
- Used when the backend process is shutting down

The function also cleans up "allocated" stdio files, directories, and file descriptors regardless of the cleanup mode, and warns about unclosed allocated descriptors if called during commit.

## Parameters / Member Variables
- `isCommit`: Boolean indicating if this is a normal transaction commit (true) or abort/other (false). Used to determine whether to warn about unclosed allocated descriptors.
- `isProcExit`: Boolean indicating if this is process exit cleanup (true) or transaction-level cleanup (false). Controls which temporary files are closed.

## Dependencies
- Functions called/Symbols referenced:
  - FileIsNotOpen (assertion check for VFD ring integrity)
  - FileClose (to close individual temporary files)
  - FreeDesc (to clean up allocated descriptors)
  - FD_DELETE_AT_CLOSE, FD_CLOSE_AT_EOXACT (file descriptor state flags)
- Called from (representative examples):
  - [AtEOXact_Files](../A/AtEOXact_Files.md) (for transaction-level cleanup)
  - [BeforeShmemExit_Files](../B/BeforeShmemExit_Files.md) (for process exit cleanup)

## Notes and Other Information
- This is a static function only accessible within fd.c
- The function handles two types of temporary files: transaction-local (FD_CLOSE_AT_EOXACT) and persistent (FD_DELETE_AT_CLOSE)
- Uses have_xact_temporary_files flag to optimize performance by skipping cleanup when no transaction-local temp files exist
- Provides debugging warnings to help identify resource leaks
- Works in conjunction with ResourceOwner system for comprehensive resource management
- The VfdCache array iteration skips index 0 as it's not used for actual files