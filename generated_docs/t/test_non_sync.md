# test_non_sync

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:574-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L574-L598)

## Overview
A baseline benchmarking function that measures the performance of simple write operations without any synchronization, providing a reference point for comparing the overhead of various sync methods.

## Definition
static void test_non_sync(void)

## Detailed Description
The test_non_sync function performs a straightforward benchmark test that measures the raw write performance without any form of synchronization (no fsync, fdatasync, or synchronous open flags). This test serves as a crucial baseline measurement that allows users to understand the performance cost associated with different synchronization methods tested by other functions in the pg_test_fsync utility.

The function performs repeated write operations of XLOG_BLCKSZ bytes (typically 8KB) to the same file position (offset 0), effectively overwriting the same data location repeatedly. Since no synchronization is performed, the writes may be buffered by the operating system and not immediately persisted to storage, representing the fastest possible write scenario.

This baseline measurement is essential for calculating the relative overhead of synchronization methods. For example, if non-sync writes achieve 1000 ops/sec and fsync writes achieve 100 ops/sec, the synchronization overhead is 10x.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - printf
  - fflush
  - open
  - [pg_pwrite](../p/pg_pwrite.md)
  - close
  - [die](../d/die.md)
- Macros used:
  - XLOG_BLCKSZ_K
  - LABEL_FORMAT
  - START_TIMER
  - STOP_TIMER
  - PG_BINARY
  - XLOG_BLCKSZ
- Called from:
  - [main](../m/main.md) (in pg_test_fsync.c)

## Notes and Other Information
- Provides the theoretical maximum write performance for the given hardware and filesystem
- Essential baseline for understanding the true cost of data durability guarantees
- Results represent buffered write performance and may not reflect actual disk throughput
- Always writes to the same file offset (0), which may affect caching behavior
- Uses pg_pwrite for position-independent writes, consistent with other test functions
- The lack of synchronization means data may be lost if the system crashes before OS buffers are flushed
- Part of a comprehensive suite of tests that help PostgreSQL administrators make informed decisions about wal_sync_method settings
- Results can vary significantly based on available system memory, filesystem type, and storage hardware
- Critical for understanding the performance trade-offs between data safety and write speed in PostgreSQL