# TouchSocketLockFiles

## Location
[src/backend/utils/init/miscinit.c:1537-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1537-L1565)

## Overview
Updates the modification and access timestamps of socket lock files to prevent their removal by system cleanup daemons.

## Definition
```c
void TouchSocketLockFiles(void)
```

## Detailed Description
This function iterates through all registered lock files and updates their timestamps using the utime() system call. The primary purpose is to prevent overzealous temporary directory cleanup daemons from removing socket lock files due to perceived inactivity. This is particularly important for Unix domain sockets that are often placed in /tmp directories. The function specifically skips the data directory lock file (DIRECTORY_LOCK_FILE) as it's considered sufficiently protected. Any errors during the timestamp update are silently ignored to avoid disrupting normal operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - DIRECTORY_LOCK_FILE (constant)
  - lock_files (global list variable)
  - utime() (system call)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md)

## Notes and Other Information
- Called periodically during normal server operation to maintain lock file freshness
- Errors during timestamp updates are intentionally ignored
- Skips the data directory lock file as it doesn't need timestamp refreshing
- Essential for preventing socket lock file removal in /tmp directories
- Part of the server's maintenance routine in the main event loop

## Simplified Source

```c
// Simplified version of TouchSocketLockFiles
void TouchSocketLockFiles(void) {
    // Iterate through all registered lock files
    foreach(l, lock_files) {
        char *socketLockFile = (char *) lfirst(l);

        // Skip data directory lock file - doesn't need refreshing
        if (strcmp(socketLockFile, DIRECTORY_LOCK_FILE) == 0)
            continue;

        // Update file timestamp to prevent cleanup daemon removal
        // Ignore any errors - don't disrupt normal operations
        (void) utime(socketLockFile, NULL);
    }
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Consolidated the core logic flow with clear explanations
- Preserved the essential algorithm and error handling approach
- Maintained the skip condition for data directory lock files
- Kept the intentional error ignoring behavior