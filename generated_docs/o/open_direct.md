# open_direct

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:265-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L265-L289)

## Overview
The open_direct function provides a cross-platform wrapper for opening files with direct I/O capabilities, bypassing operating system buffer cache when available.

## Definition
```c
static int open_direct(const char *path, int flags, mode_t mode)
```

## Detailed Description
This function attempts to open a file with direct I/O enabled to bypass the operating system's buffer cache. It provides cross-platform compatibility by supporting both Linux's O_DIRECT flag and macOS/BSD's F_NOCACHE fcntl option. When O_DIRECT is available, it's added to the flags before calling open(). On platforms that support F_NOCACHE but not O_DIRECT, it opens the file normally then applies the F_NOCACHE setting via fcntl(). This enables accurate filesystem sync performance testing by avoiding cache effects that could skew results.

## Parameters / Member Variables
- `path`: File path to open
- `flags`: File opening flags (O_RDWR, O_CREAT, etc.)
- `mode`: File permissions when creating (mode_t type)

## Dependencies
- Functions called/Symbols referenced:
  - open (POSIX file opening)
  - fcntl (POSIX file control, F_NOCACHE variant)
  - close (POSIX file closing)
  - O_DIRECT (Linux direct I/O flag)
  - F_NOCACHE (BSD/macOS cache bypass flag)
- Called from (representative examples):
  - [test_sync](../t/test_sync.md) (filesystem sync testing function)
  - [test_open_sync](../t/test_open_sync.md) (open+sync testing function)

## Notes and Other Information
- Returns file descriptor on success, -1 on failure (standard POSIX convention)
- Automatically adds O_DIRECT flag when available on the platform
- Falls back to F_NOCACHE via fcntl() on BSD/macOS systems
- Preserves errno on F_NOCACHE failures by saving and restoring it
- Closes file descriptor and returns -1 if F_NOCACHE setting fails
- Platform-specific conditional compilation ensures compatibility across different operating systems
- Essential for accurate I/O performance testing by eliminating cache effects
- File location: src/bin/pg_test_fsync/pg_test_fsync.c:265-289

## Simplified Source

```c
static int
open_direct(const char *path, int flags, mode_t mode)
{
    int fd;

    // Enable direct I/O if supported (Linux)
#ifdef O_DIRECT
    flags |= O_DIRECT;
#endif

    // Open the file
    fd = open(path, flags, mode);

    // Alternative direct I/O method for BSD/macOS
#if !defined(O_DIRECT) && defined(F_NOCACHE)
    if (fd >= 0 && fcntl(fd, F_NOCACHE, 1) < 0) {
        int save_errno = errno;
        close(fd);
        errno = save_errno;
        return -1;
    }
#endif

    return fd;
}
```