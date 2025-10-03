# prepare_buf

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:231-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L231-L242)

## Overview
The prepare_buf function initializes a buffer with random data for filesystem sync testing, ensuring proper alignment for PostgreSQL WAL block operations.

## Definition
```c
static void prepare_buf(void)
```

## Detailed Description
This function prepares a test buffer by filling it with random data and setting up proper alignment for filesystem sync tests. It fills the full_buf array with random bytes using PostgreSQL's pseudo-random number generator, then creates an aligned pointer (buf) that points to a properly aligned location within full_buf. The alignment is set to XLOG_BLCKSZ (WAL block size) which is critical for accurate testing of PostgreSQL's write-ahead logging performance characteristics.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_XLOG_SEG_SIZE (default WAL segment size constant)
  - [pg_prng_int32](pg_prng_int32.md) (PostgreSQL pseudo-random number generator)
  - TYPEALIGN (PostgreSQL alignment macro)
  - pg_global_prng_state (global PRNG state)
- Called from (representative examples):
  - [main](../m/main.md) (pg_test_fsync main function)

## Notes and Other Information
- Operates on global variables full_buf and buf
- Uses random data to avoid filesystem optimization tricks that might cache or compress predictable patterns
- The TYPEALIGN macro ensures the buffer is aligned to XLOG_BLCKSZ boundaries for realistic WAL testing
- The buffer size is based on DEFAULT_XLOG_SEG_SIZE to match PostgreSQL's actual WAL segment size
- Essential setup function that must be called before running filesystem sync tests
- File location: src/bin/pg_test_fsync/pg_test_fsync.c:231-242