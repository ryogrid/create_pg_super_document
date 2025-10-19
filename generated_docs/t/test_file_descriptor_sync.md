# test_file_descriptor_sync

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:505-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L505-L573)

## Overview
A benchmarking function that tests whether fsync operations can effectively synchronize data written through different file descriptors to the same file, simulating multi-process synchronization scenarios.

## Definition
static void test_file_descriptor_sync(void)

## Detailed Description
The test_file_descriptor_sync function performs a critical test to determine the efficiency of cross-descriptor fsync operations in multi-process environments. This test is particularly important for PostgreSQL's multi-process architecture where different processes may need to synchronize writes made by other processes to the same file.

The function performs two contrasting tests:

1. **Normal Behavior Test ("write, fsync, close")**: 
   - Opens file, writes data, fsyncs on the same descriptor, then closes
   - This represents the standard, expected behavior for file synchronization
   - Also includes an additional open/close cycle for consistency with the second test

2. **Cross-Descriptor Sync Test ("write, close, fsync")**:
   - Opens file, writes data, closes the descriptor
   - Reopens the same file with a different descriptor
   - Attempts to fsync using the new descriptor to sync data written by the previous descriptor

By comparing the performance of these two approaches, the test reveals whether the operating system and filesystem can efficiently handle fsync operations on file descriptors that didn't perform the original write operations. Similar performance indicates that fsync can effectively sync data written on different descriptors, which is crucial for multi-process database systems.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - printf
  - fflush
  - open
  - write
  - fsync
  - close
  - [die](../d/die.md)
- Macros used:
  - LABEL_FORMAT
  - START_TIMER
  - STOP_TIMER
  - PG_BINARY
  - XLOG_BLCKSZ
- Called from:
  - [main](../m/main.md) (in pg_test_fsync.c)

## Notes and Other Information
- Critical for understanding multi-process fsync behavior in PostgreSQL environments
- Tests a filesystem and OS capability that directly impacts PostgreSQL's multi-process architecture
- The comment suggests this test might be enhanced with writethrough on supporting platforms
- Results help determine if separate processes can reliably fsync each other's writes
- Similar timing between the two tests indicates efficient cross-descriptor fsync support
- Each test iteration writes exactly XLOG_BLCKSZ bytes to maintain consistency
- Uses the standard alarm-based timing mechanism for accurate performance measurement
- Important for PostgreSQL installations where multiple backend processes write to shared files
- The test design accounts for potential filesystem caching effects by reopening files

## Simplified Source

```c
static void test_file_descriptor_sync(void)
{
    int tmpfile, ops;

    printf(_("\nTest if fsync on non-write file descriptor is honored:\n"));
    printf(_("(If the times are similar, fsync() can sync data written on a different\n"
             "descriptor.)\n"));

    // Test 1: Normal behavior - write, fsync, close on same descriptor
    printf(LABEL_FORMAT, "write, fsync, close");
    fflush(stdout);

    START_TIMER;
    for (ops = 0; alarm_triggered == false; ops++) {
        // Open, write, fsync, and close on same descriptor
        tmpfile = open(filename, O_RDWR | PG_BINARY, 0);
        if (tmpfile == -1) die("could not open output file");

        if (write(tmpfile, buf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
            die("write failed");
        if (fsync(tmpfile) != 0)
            die("fsync failed");
        close(tmpfile);

        // Additional open/close for consistency
        tmpfile = open(filename, O_RDWR | PG_BINARY, 0);
        if (tmpfile == -1) die("could not open output file");
        close(tmpfile);
    }
    STOP_TIMER;

    // Test 2: Cross-descriptor sync - write on one descriptor, fsync on another
    printf(LABEL_FORMAT, "write, close, fsync");
    fflush(stdout);

    START_TIMER;
    for (ops = 0; alarm_triggered == false; ops++) {
        // Write and close
        tmpfile = open(filename, O_RDWR | PG_BINARY, 0);
        if (tmpfile == -1) die("could not open output file");

        if (write(tmpfile, buf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
            die("write failed");
        close(tmpfile);

        // Reopen and fsync on different descriptor
        tmpfile = open(filename, O_RDWR | PG_BINARY, 0);
        if (tmpfile == -1) die("could not open output file");

        if (fsync(tmpfile) != 0)
            die("fsync failed");
        close(tmpfile);
    }
    STOP_TIMER;
}
```