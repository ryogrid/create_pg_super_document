# AddToDataDirLockFile

## Location
[src/backend/utils/init/miscinit.c:1566-1692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1566-L1692)

## Overview
Adds or replaces a specific line in the data directory lock file with atomic write operations to maintain consistency.

## Definition
```c
void AddToDataDirLockFile(int target_line, const char *str)
```

## Detailed Description
This function performs an atomic update of a specific line in the data directory lock file (typically "postmaster.pid"). It reads the entire lock file into memory, modifies the specified line with the given string, and writes the entire content back in a single operation. The function ensures atomicity by performing the write in one kernel call and includes proper error handling and wait event reporting for monitoring. It handles cases where lines are added out of order by filling in missing lines with newlines. The implementation intentionally avoids truncating the file to maintain atomic updates, which means callers should avoid shortening lines once written.

## Parameters / Member Variables
- `target_line`: The line number (1-based) in the lock file to add or replace
- `str`: The string content to write to the specified line (should not include trailing newline)

## Dependencies
- Functions called/Symbols referenced:
  - open
  - read
  - [pg_pwrite](../p/pg_pwrite.md)
  - [pg_fsync](../p/pg_fsync.md)
  - close
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - DIRECTORY_LOCK_FILE (constant)
  - PG_BINARY (constant)
- Called from (representative examples):
  - [InternalIpcMemoryCreate](../I/InternalIpcMemoryCreate.md)
  - [PostmasterMain](../P/PostmasterMain.md)
  - [process_pm_shutdown_request](../p/process_pm_shutdown_request.md)
  - [process_pm_child_exit](../p/process_pm_child_exit.md)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md)

## Notes and Other Information
- Updates are atomic due to single kernel call write operation
- File is not truncated to maintain atomicity, so callers should avoid shortening lines
- Includes comprehensive error handling with appropriate logging
- Uses wait events for monitoring I/O operations
- Handles out-of-order line additions by filling gaps with newlines
- Critical for maintaining lock file consistency during server state changes

## Simplified Source

```c
// Simplified version of AddToDataDirLockFile
void AddToDataDirLockFile(int target_line, const char *str) {
    int fd;
    int len;
    int lineno;
    char srcbuffer[BLCKSZ];
    char destbuffer[BLCKSZ];
    char *srcptr;
    char *destptr;

    // Step 1: Open the lock file for read/write
    fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);
    if (fd < 0) {
        ereport(LOG, (errmsg("could not open lock file")));
        return;
    }

    // Step 2: Read entire file content into buffer
    len = read(fd, srcbuffer, sizeof(srcbuffer) - 1);
    if (len < 0) {
        ereport(LOG, (errmsg("could not read lock file")));
        close(fd);
        return;
    }
    srcbuffer[len] = '\0';

    // Step 3: Copy lines before target line to destination buffer
    srcptr = srcbuffer;
    for (lineno = 1; lineno < target_line; lineno++) {
        char *eol = strchr(srcptr, '\n');
        if (eol == NULL)
            break;  // Not enough lines in file yet
        srcptr = eol + 1;
    }
    memcpy(destbuffer, srcbuffer, srcptr - srcbuffer);
    destptr = destbuffer + (srcptr - srcbuffer);

    // Step 4: Fill in any missing lines with newlines
    for (; lineno < target_line; lineno++) {
        if (destptr < destbuffer + sizeof(destbuffer))
            *destptr++ = '\n';
    }

    // Step 5: Write the target line content
    snprintf(destptr, destbuffer + sizeof(destbuffer) - destptr, "%s\n", str);
    destptr += strlen(destptr);

    // Step 6: Append any remaining lines from original file
    if ((srcptr = strchr(srcptr, '\n')) != NULL) {
        srcptr++;
        snprintf(destptr, destbuffer + sizeof(destbuffer) - destptr, "%s", srcptr);
    }

    // Step 7: Write entire content back atomically
    len = strlen(destbuffer);
    if (pg_pwrite(fd, destbuffer, len, 0) != len) {
        ereport(LOG, (errmsg("could not write to lock file")));
        close(fd);
        return;
    }

    // Step 8: Sync to disk and close file
    pg_fsync(fd);
    close(fd);
}
```

Key simplifications made:
- Removed detailed error handling for each system call (kept essential error reporting)
- Consolidated wait event reporting calls
- Simplified error messages to focus on main actions
- Abstracted errno handling details
- Focused on the main execution path while preserving the atomic update logic
- Maintained the core algorithm: read → modify → write atomically