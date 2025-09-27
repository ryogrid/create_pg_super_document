# RecheckDataDirLockFile

## Location
[src/backend/utils/init/miscinit.c:1693-1764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1693-L1764)

## Overview
Periodically verifies that the data directory lock file still exists and contains the expected process ID to detect unauthorized access or lock file tampering.

## Definition
```c
bool RecheckDataDirLockFile(void)
```

## Detailed Description
This function performs a safety check by reading the data directory lock file and verifying that it contains the current process's PID. It's designed to detect scenarios where another PostgreSQL instance might have overwritten the lock file or where the file has been removed or corrupted. The function is intentionally conservative, returning true (indicating safety) in cases of transient errors to avoid unnecessary panic shutdowns. Only clear evidence of problems (like file not found or wrong PID) results in a false return value, which would trigger emergency shutdown procedures.

## Parameters / Member Variables
- Returns: `bool` - true if lock file appears valid or errors are non-fatal, false if there's clear evidence of lock file corruption or replacement

## Dependencies
- Functions called/Symbols referenced:
  - open
  - read
  - close
  - getpid
  - atol
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - DIRECTORY_LOCK_FILE (constant)
  - PG_BINARY (constant)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md)

## Notes and Other Information
- Called periodically during normal postmaster operation for safety monitoring
- Designed to be conservative - treats transient errors as non-fatal to avoid unnecessary shutdowns
- Only triggers panic shutdown on clear evidence of lock file problems (ENOENT, ENOTDIR, wrong PID)
- Critical safety mechanism to prevent multiple PostgreSQL instances from accessing the same data directory
- Returns false only when there's definitive evidence of lock file compromise

## Simplified Source

```c
// Simplified version of RecheckDataDirLockFile
bool RecheckDataDirLockFile(void) {
    int fd;
    int len;
    long file_pid;
    char buffer[BLCKSZ];

    // Try to open the lock file
    fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);
    if (fd < 0) {
        // Only treat file-not-found errors as fatal
        if (errno == ENOENT || errno == ENOTDIR) {
            ereport(LOG, (errmsg("could not open lock file: disaster")));
            return false;
        }
        // All other errors are treated as transient
        return true;
    }

    // Read the PID from the lock file
    len = read(fd, buffer, sizeof(buffer) - 1);
    if (len < 0) {
        close(fd);
        return true;  // Treat read failure as non-fatal
    }

    // Parse the PID and compare with current process
    buffer[len] = '\0';
    close(fd);
    file_pid = atol(buffer);

    if (file_pid == getpid()) {
        return true;  // Lock file contains our PID - all is well
    }

    // Wrong PID in lock file - someone else overwrote it
    ereport(LOG, (errmsg("lock file contains wrong PID: %ld instead of %ld",
                         file_pid, (long) getpid())));
    return false;
}
```

Key simplifications made:
- Removed detailed error logging for non-fatal cases
- Consolidated switch statement into simple if-else logic
- Removed statistics reporting calls for brevity
- Simplified error messages while preserving essential information
- Focused on the main execution path: open → read → compare PID
- Maintained the conservative approach of treating ambiguous errors as non-fatal