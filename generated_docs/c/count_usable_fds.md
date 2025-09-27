# count_usable_fds

## Location
[src/backend/storage/file/fd.c:961-1040](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L961-L1040)

## Overview
Counts how many file descriptors the system will allow PostgreSQL to open and estimates how many are already in use by systematically attempting to duplicate file descriptors.

## Definition
```c
static void count_usable_fds(int max_to_probe, int *usable_fds, int *already_open)
```

## Detailed Description
This function performs file descriptor availability testing by repeatedly calling `dup(2)` (duplicating stderr) until it fails or reaches the specified probe limit. It uses this approach to determine both the practical limit of file descriptors the process can open and estimate how many are currently in use.

The function employs a systematic probing strategy:
1. Attempts to duplicate stderr (FD 2) repeatedly
2. Tracks the highest file descriptor number achieved
3. Stops when `dup()` fails (typically with EMFILE/ENFILE) or max_to_probe is reached
4. Calculates usable FDs and estimates already-open FDs based on the gap between highest FD and successful duplications

The function respects system limits by checking RLIMIT_NOFILE when available and avoids going beyond the hard limit to prevent kernel log spam.

## Parameters / Member Variables
- `max_to_probe`: Maximum number of file descriptors to test before stopping the probe
- `usable_fds`: Output parameter - number of file descriptors successfully duplicated (available for use)
- `already_open`: Output parameter - estimated number of file descriptors already in use by the process

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [pfree](../p/pfree.md)
  - dup
  - close
  - getrlimit (when HAVE_GETRLIMIT is defined)
  - ereport
  - elog
- Called from (representative examples):
  - [set_max_safe_fds](../s/set_max_safe_fds.md)

## Notes and Other Information
- Assumes stderr (FD 2) is available for duplication - closing stderr would break this function
- Uses dynamic memory allocation to store duplicated FDs, starting with 1024 entries and doubling as needed
- The calculation `already_open = highestfd + 1 - used` assumes FDs are numbered starting from 0
- Small values of max_to_probe may underestimate already_open due to gaps in the FD space
- Function is static and only used internally within the file descriptor management subsystem
- Handles platform differences through conditional compilation (HAVE_GETRLIMIT)

## Simplified Source

```c
// Simplified version of count_usable_fds
static void count_usable_fds(int max_to_probe, int *usable_fds, int *already_open) {
    int *fd_array;
    int array_size = 1024;
    int used_fds = 0;
    int highest_fd = 0;

    // Allocate array to track duplicated file descriptors
    fd_array = (int *) palloc(array_size * sizeof(int));

    // Check system FD limit if available
    struct rlimit fd_limit;
    bool has_limit = (getrlimit(RLIMIT_NOFILE, &fd_limit) == 0);

    // Keep duplicating stderr (FD 2) until failure or limit reached
    for (;;) {
        // Don't exceed system limit to avoid kernel warnings
        if (has_limit && highest_fd >= fd_limit.rlim_cur - 1) {
            break;
        }

        // Try to duplicate stderr
        int new_fd = dup(2);
        if (new_fd < 0) {
            // Failed - we've hit the FD limit
            break;
        }

        // Expand array if needed
        if (used_fds >= array_size) {
            array_size *= 2;
            fd_array = (int *) repalloc(fd_array, array_size * sizeof(int));
        }

        // Store this FD and update tracking
        fd_array[used_fds++] = new_fd;
        if (highest_fd < new_fd) {
            highest_fd = new_fd;
        }

        // Stop if we've probed enough
        if (used_fds >= max_to_probe) {
            break;
        }
    }

    // Clean up: close all duplicated FDs
    for (int i = 0; i < used_fds; i++) {
        close(fd_array[i]);
    }
    pfree(fd_array);

    // Calculate results
    *usable_fds = used_fds;
    *already_open = highest_fd + 1 - used_fds;
}
```

Key simplifications made:
- Removed detailed error handling and warning messages for clarity
- Consolidated platform-specific conditional compilation into simpler logic
- Used more descriptive variable names (fd_array, used_fds, highest_fd)
- Added inline comments explaining each major step
- Simplified the main loop structure while preserving core algorithm
- Abstracted the memory management details while keeping the essential resize logic