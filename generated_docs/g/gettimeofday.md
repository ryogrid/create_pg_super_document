# gettimeofday

## Location
[src/port/win32gettimeofday.c:53-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32gettimeofday.c#L53-L75)

## Overview
A Windows-specific replacement for the POSIX  system call that provides current system time with microsecond precision.

## Definition

```c
int
gettimeofday(struct timeval *tp, void *tzp)
```
## Detailed Description
This function is a Windows-specific implementation of the POSIX  system call, located in . It converts Windows FILETIME format to the POSIX  structure format.

The implementation uses Windows API  to get high-precision system time, then converts it from Windows epoch (January 1, 1601) to POSIX epoch (January 1, 1970). The function ensures portability by asserting that the timezone parameter is NULL, as POSIX behavior for this parameter is unspecified and PostgreSQL doesn't rely on any non-portable behavior.

This implementation is not intended for high-precision timing operations within PostgreSQL - for such purposes, the  function should be used instead.

## Parameters / Member Variables
- `*tp`: Pointer to  where the current time will be stored, with  (seconds) and  (microseconds) fields
- `*tzp`: Timezone parameter (must be NULL); PostgreSQL asserts this to ensure portable behavior
## Dependencies
- Functions called/Symbols referenced:
  -  (Windows API)
  -  (PostgreSQL assertion macro)
  -  (constant: 10,000,000L)
  -  (constant: 10)
  -  (static constant: Windows to POSIX epoch conversion)

- Called from (representative examples):
  -  (src/backend/access/transam/xlog.c:5015)
  -  (src/backend/libpq/auth.c:3108, 3118)
  -  (src/backend/postmaster/checkpointer.c:840)
  -  (src/backend/utils/adt/timestamp.c:1659)
  -  (src/backend/utils/adt/timestamp.c:1707)
  -  (src/backend/utils/error/elog.c:2665)
  -  (src/backend/utils/misc/pg_rusage.c:30)
  - Various utilities in pg_basebackup, pg_resetwal, pg_test_fsync, pgbench, libpq, and isolation testing

## Notes and Other Information
- This is a Win32-specific portability function that only exists on Windows builds of PostgreSQL
- The function performs conversion from Windows FILETIME (100-nanosecond intervals since Jan 1, 1601) to POSIX timeval format
- Returns 0 on success, following POSIX conventions
- The timezone parameter assertion ensures PostgreSQL code remains portable across platforms
- Not suitable for high-precision timing measurements - use  for performance-critical timing operations
- Part of PostgreSQL's platform abstraction layer for Windows compatibility

## Simplified Source

```c
// Simplified version of gettimeofday
int gettimeofday(struct timeval *tp, void *tzp) {
    FILETIME file_time;
    ULARGE_INTEGER ularge;

    // Ensure timezone parameter is NULL for portability
    Assert(tzp == NULL);

    // Get high-precision system time from Windows
    GetSystemTimePreciseAsFileTime(&file_time);

    // Convert FILETIME to 64-bit integer for easier manipulation
    ularge.LowPart = file_time.dwLowDateTime;
    ularge.HighPart = file_time.dwHighDateTime;

    // Convert from Windows epoch (1601) to POSIX epoch (1970) and extract seconds
    tp->tv_sec = (long) ((ularge.QuadPart - epoch) / FILETIME_UNITS_PER_SEC);

    // Extract microseconds from remainder
    tp->tv_usec = (long) (((ularge.QuadPart - epoch) % FILETIME_UNITS_PER_SEC)
                          / FILETIME_UNITS_PER_USEC);

    return 0;
}
```

Key simplifications made:
- Added explanatory comments for each major step
- Preserved the essential Windows FILETIME to POSIX timeval conversion logic
- Kept the portability assertion as it's critical for PostgreSQL's cross-platform compatibility
- Maintained the precise time conversion calculations
- Focused on the main execution path without losing important functionality