# test_open_sync

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:469-504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L469-L504)

## Overview
A specialized benchmarking function that tests the performance of open_sync operations with a specific write size configuration, measuring how efficiently data can be written using synchronous I/O.

## Definition
static void test_open_sync(const char *msg, int writes_size)

## Detailed Description
The test_open_sync function performs a focused benchmark test on the open_sync synchronization method using a specified write size. It opens a file with the O_SYNC flag, which ensures that each write operation is synchronized to storage before the write call returns. The function always writes a total of 16kB of data, but the number of write operations and the size of each write depend on the writes_size parameter.

The function uses conditional compilation to only run when O_SYNC is available on the platform. It performs repeated write operations within a timed loop, with each iteration writing 16kB of data in chunks of the specified size. For example, if writes_size is 4, it will perform 4 writes of 4kB each per iteration.

The function handles direct I/O through the open_direct function and provides appropriate error handling for cases where the file cannot be opened with the requested flags.

## Parameters / Member Variables
- `msg`: A descriptive message string to display before running the test (e.g., "1 * 16kB open_sync write")
- `writes_size`: The size of each individual write operation in kilobytes (determines how 16kB total is split)

## Dependencies
- Functions called/Symbols referenced:
  - printf
  - fflush  
  - [open_direct](../o/open_direct.md)
  - [pg_pwrite](../p/pg_pwrite.md)
  - close
  - [die](../d/die.md)
- Macros used:
  - LABEL_FORMAT
  - NA_FORMAT
  - START_TIMER
  - STOP_TIMER
  - O_SYNC (conditional compilation)
  - PG_BINARY
- Called from:
  - [test_open_syncs](test_open_syncs.md) (5 times with different write sizes)

## Notes and Other Information
- Only available on platforms that support O_SYNC flag for file operations
- Always writes exactly 16kB per iteration, but varies the write pattern based on writes_size parameter
- Uses direct I/O when possible through open_direct function
- Part of a comprehensive suite of sync method benchmarks in pg_test_fsync utility
- The writes_size parameter directly affects performance characteristics - larger writes typically have lower overhead
- Uses alarm-based timing mechanism consistent with other test functions
- Provides "n/a*" output when direct I/O is not supported by the filesystem
- Critical for determining optimal write sizes when using synchronous I/O in PostgreSQL

## Simplified Source

```c
static void test_open_sync(const char *msg, int writes_size)
{
    int tmpfile, ops, writes;

    printf(LABEL_FORMAT, msg);
    fflush(stdout);

#ifdef O_SYNC
    // Open file with synchronous I/O flag
    tmpfile = open_direct(filename, O_RDWR | O_SYNC | PG_BINARY, 0);

    if (tmpfile == -1) {
        printf(NA_FORMAT, _("n/a*"));
        return;
    }

    // Time repeated write operations
    START_TIMER;
    for (ops = 0; alarm_triggered == false; ops++) {
        // Write 16kB total in chunks of writes_size
        for (writes = 0; writes < 16 / writes_size; writes++) {
            if (pg_pwrite(tmpfile, buf, writes_size * 1024,
                         writes * writes_size * 1024) != writes_size * 1024) {
                die("write failed");
            }
        }
    }
    STOP_TIMER;
    close(tmpfile);
#else
    printf(NA_FORMAT, _("n/a"));
#endif
}
```