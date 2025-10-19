# test_sync

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:290-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L290-L451)

## Overview
A comprehensive benchmarking function that tests and compares different file synchronization methods to help determine the most efficient wal_sync_method for PostgreSQL's write-ahead log operations.

## Definition
static void test_sync(int writes_per_op)

## Detailed Description
The test_sync function performs performance benchmarks on various file synchronization methods used in PostgreSQL, specifically for testing wal_sync_method options. It tests each synchronization method by performing repeated write operations followed by the appropriate sync operation, measuring the time taken for each method. The function tests methods in the order of PostgreSQL's wal_sync_method preference, though fdatasync is noted as Linux's default.

The function tests the following sync methods when available:
1. open_datasync - Opens file with O_DSYNC flag for immediate data synchronization
2. fdatasync - Synchronizes data but not necessarily metadata
3. fsync - Synchronizes both data and metadata
4. fsync_writethrough - Platform-specific writethrough synchronization
5. open_sync - Opens file with O_SYNC flag for full synchronization

Each test writes XLOG_BLCKSZ-sized blocks (typically 8KB) and measures performance using timer macros. The function handles platform-specific availability of sync methods and provides appropriate "n/a" messages when methods are not supported.

## Parameters / Member Variables
- `writes_per_op`: Number of write operations to perform before each sync operation (typically 1 or 2)

## Dependencies
- Functions called/Symbols referenced:
  - printf
  - fflush
  - [open_direct](../o/open_direct.md)
  - open
  - [pg_pwrite](../p/pg_pwrite.md)
  - [fdatasync](../f/fdatasync.md)
  - fsync
  - [pg_fsync_writethrough](../p/pg_fsync_writethrough.md)
  - close
  - [die](../d/die.md)
- Macros used:
  - START_TIMER
  - STOP_TIMER
  - LABEL_FORMAT
  - NA_FORMAT
  - XLOG_BLCKSZ_K
  - XLOG_BLCKSZ
- Called from:
  - [main](../m/main.md) (in pg_test_fsync.c)

## Notes and Other Information
- Uses conditional compilation for platform-specific sync methods (O_DSYNC, O_SYNC, HAVE_FSYNC_WRITETHROUGH)
- Provides warnings about filesystem limitations for direct I/O operations
- Part of the pg_test_fsync utility which helps database administrators choose optimal sync methods
- Uses alarm-based timing mechanism to ensure consistent test duration across different sync methods
- Handles write failures gracefully and provides informative error messages
- The function is critical for PostgreSQL performance tuning as the choice of sync method significantly impacts WAL performance

## Simplified Source

```c
static void
test_sync(int writes_per_op)
{
    int tmpfile, ops, writes;
    bool fs_warning = false;

    // Display test header
    if (writes_per_op == 1)
        printf("Compare file sync methods using one %dkB write:\n", XLOG_BLCKSZ_K);
    else
        printf("Compare file sync methods using two %dkB writes:\n", XLOG_BLCKSZ_K);
    printf("(in \"wal_sync_method\" preference order, except fdatasync is Linux's default)\n");

    // Test open_datasync (O_DSYNC) if available
    printf(LABEL_FORMAT, "open_datasync");
    fflush(stdout);

#ifdef O_DSYNC
    if ((tmpfile = open_direct(filename, O_RDWR | O_DSYNC | PG_BINARY, 0)) == -1)
    {
        printf(NA_FORMAT, "n/a*");
        fs_warning = true;
    }
    else
    {
        START_TIMER;
        for (ops = 0; alarm_triggered == false; ops++)
        {
            for (writes = 0; writes < writes_per_op; writes++)
                if (pg_pwrite(tmpfile, buf, XLOG_BLCKSZ, writes * XLOG_BLCKSZ) != XLOG_BLCKSZ)
                    die("write failed");
        }
        STOP_TIMER;
        close(tmpfile);
    }
#else
    printf(NA_FORMAT, "n/a");
#endif

    // Test fdatasync
    printf(LABEL_FORMAT, "fdatasync");
    fflush(stdout);

    if ((tmpfile = open(filename, O_RDWR | PG_BINARY, 0)) == -1)
        die("could not open output file");
    START_TIMER;
    for (ops = 0; alarm_triggered == false; ops++)
    {
        for (writes = 0; writes < writes_per_op; writes++)
            if (pg_pwrite(tmpfile, buf, XLOG_BLCKSZ, writes * XLOG_BLCKSZ) != XLOG_BLCKSZ)
                die("write failed");
        fdatasync(tmpfile);
    }
    STOP_TIMER;
    close(tmpfile);

    // Test fsync
    printf(LABEL_FORMAT, "fsync");
    fflush(stdout);

    if ((tmpfile = open(filename, O_RDWR | PG_BINARY, 0)) == -1)
        die("could not open output file");
    START_TIMER;
    for (ops = 0; alarm_triggered == false; ops++)
    {
        for (writes = 0; writes < writes_per_op; writes++)
            if (pg_pwrite(tmpfile, buf, XLOG_BLCKSZ, writes * XLOG_BLCKSZ) != XLOG_BLCKSZ)
                die("write failed");
        if (fsync(tmpfile) != 0)
            die("fsync failed");
    }
    STOP_TIMER;
    close(tmpfile);

    // Test fsync_writethrough if available
    printf(LABEL_FORMAT, "fsync_writethrough");
    fflush(stdout);

#ifdef HAVE_FSYNC_WRITETHROUGH
    if ((tmpfile = open(filename, O_RDWR | PG_BINARY, 0)) == -1)
        die("could not open output file");
    START_TIMER;
    for (ops = 0; alarm_triggered == false; ops++)
    {
        for (writes = 0; writes < writes_per_op; writes++)
            if (pg_pwrite(tmpfile, buf, XLOG_BLCKSZ, writes * XLOG_BLCKSZ) != XLOG_BLCKSZ)
                die("write failed");
        if (pg_fsync_writethrough(tmpfile) != 0)
            die("fsync failed");
    }
    STOP_TIMER;
    close(tmpfile);
#else
    printf(NA_FORMAT, "n/a");
#endif

    // Test open_sync (O_SYNC) if available
    printf(LABEL_FORMAT, "open_sync");
    fflush(stdout);

#ifdef O_SYNC
    if ((tmpfile = open_direct(filename, O_RDWR | O_SYNC | PG_BINARY, 0)) == -1)
    {
        printf(NA_FORMAT, "n/a*");
        fs_warning = true;
    }
    else
    {
        START_TIMER;
        for (ops = 0; alarm_triggered == false; ops++)
        {
            for (writes = 0; writes < writes_per_op; writes++)
                if (pg_pwrite(tmpfile, buf, XLOG_BLCKSZ, writes * XLOG_BLCKSZ) != XLOG_BLCKSZ)
                    die("write failed");
        }
        STOP_TIMER;
        close(tmpfile);
    }
#else
    printf(NA_FORMAT, "n/a");
#endif

    // Display filesystem warning if needed
    if (fs_warning)
    {
        printf("* This file system and its mount options do not support direct\n"
               "  I/O, e.g. ext4 in journaled mode.\n");
    }
}
```