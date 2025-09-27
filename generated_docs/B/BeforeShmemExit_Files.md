# BeforeShmemExit_Files

## Location
[src/backend/storage/file/fd.c:3176-3198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3176-L3198)

## Overview
A cleanup hook function that ensures all temporary files are removed during backend process shutdown, including inter-transaction temporary files.

## Definition
```c
static void BeforeShmemExit_Files(int code, Datum arg)
```

## Detailed Description
This function serves as a before_shmem_exit hook that performs comprehensive temporary file cleanup during backend process shutdown. Unlike transaction-level cleanup functions, this function is designed to clean up ALL temporary files, including those that span multiple transactions (inter-transaction temporary files).

The function performs two main actions:
1. Calls CleanupTempFiles with parameters (false, true) to clean up all temporary files without transaction-specific filtering
2. In debug builds, sets temporary_files_allowed to false to prevent creation of new temporary files during shutdown

This ensures that no temporary files are left behind when a backend process terminates, maintaining system cleanliness and preventing disk space leaks.

## Parameters / Member Variables
- `code`: Exit code parameter (standard for exit hooks, not used in this function)
- `arg`: Datum argument parameter (standard for exit hooks, not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [CleanupTempFiles](../C/CleanupTempFiles.md) (called with false, true to clean all temp files)
  - temporary_files_allowed (debug variable set to false)
- Called from (representative examples):
  - AllocateDesc (registers this hook in src/backend/storage/file/fd.c:339)
  - [InitTemporaryFileAccess](../I/InitTemporaryFileAccess.md) (registers this hook in src/backend/storage/file/fd.c:939)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the fd.c module
- The function is registered as a before_shmem_exit hook during temporary file system initialization
- The use of (false, true) parameters to CleanupTempFiles ensures all temporary files are cleaned, not just transaction-local ones
- The temporary_files_allowed flag is only set in assertion-enabled builds for debugging purposes
- This provides a final safety net to ensure temporary files don't persist after backend process termination

## Simplified Source

```c
// Simplified version of BeforeShmemExit_Files
static void BeforeShmemExit_Files(int code, Datum arg) {
    // Clean up all temporary files, including inter-transaction ones
    CleanupTempFiles(false, true);

    // Prevent creation of new temp files (debug builds only)
#ifdef USE_ASSERT_CHECKING
    temporary_files_allowed = false;
#endif
}
```

Key simplifications made:
- Simplified the function comment to focus on main purpose
- Consolidated cleanup explanation in one comment
- Preserved debug-only code with clear comment
- Maintained essential cleanup logic