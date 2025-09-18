# test_atomic_uint64

## Location
src/test/regress/regress.c: 800 - 852

## Overview
A comprehensive static test function that validates the complete functionality of PostgreSQL's 64-bit atomic unsigned integer operations, including basic operations, compare-exchange, and bitwise manipulations.

## Definition


## Detailed Description
The  function is a thorough unit test that systematically validates all atomic operations available for 64-bit unsigned integers in PostgreSQL. Similar to its 32-bit counterpart, this function tests initialization, read/write operations, atomic arithmetic (add, subtract with both fetch-then-op and op-then-fetch variants), atomic exchange, and compare-and-swap operations. It also includes bitwise operations (AND, OR) for flag manipulation. The function includes retry logic for compare-and-exchange operations to handle potential spurious failures due to system interrupts. Unlike the 32-bit version, this test focuses on the core functionality without extensive boundary testing, but still validates the essential atomic operation patterns needed for 64-bit concurrent programming.

## Parameters / Member Variables
- No parameters: void function with no arguments
- Local variables:
  - : pg_atomic_uint64 type used for testing atomic operations
  - : uint64 used for compare-exchange operations
  - : int loop counter for retry logic

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint64: Type definition for 64-bit atomic unsigned integer
  - pg_atomic_init_u64: Initializes atomic uint64 variable
  - pg_atomic_read_u64: Atomically reads current value
  - pg_atomic_write_u64: Atomically writes new value
  - pg_atomic_fetch_add_u64: Atomically adds and returns old value
  - pg_atomic_add_fetch_u64: Atomically adds and returns new value
  - pg_atomic_fetch_sub_u64: Atomically subtracts and returns old value
  - pg_atomic_sub_fetch_u64: Atomically subtracts and returns new value
  - pg_atomic_exchange_u64: Atomically exchanges values
  - pg_atomic_compare_exchange_u64: Atomic compare-and-swap operation
  - pg_atomic_fetch_or_u64: Atomic bitwise OR with fetch
  - pg_atomic_fetch_and_u64: Atomic bitwise AND with fetch
  - EXPECT_EQ_U64: Test assertion macro for uint64 equality
  - EXPECT_TRUE: Test assertion macro for boolean conditions
- Called from (representative examples):
  - test_atomic_ops: Main atomic operations test function

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Part of PostgreSQL's regression testing framework for atomic operations
- Tests essential scenarios including:
  1. Basic initialization, read, and write operations
  2. Atomic arithmetic with both pre- and post-operation value returns
  3. Atomic exchange operations
  4. Compare-and-swap operations with both successful and failed attempts
  5. Bitwise operations for flag manipulation
- Includes a retry loop (up to 100 attempts) for compare-exchange to handle spurious failures
- Uses fewer retry attempts (100) compared to the 32-bit version (1000), potentially due to different failure characteristics on 64-bit operations
- Validates that failed compare-exchange operations leave the expected value updated with the actual value
- Tests bitwise flag operations commonly used in concurrent programming
- Located in src/test/regress/regress.c as part of PostgreSQL's test infrastructure
- Critical for ensuring 64-bit atomic operations work correctly across different CPU architectures, especially on 32-bit systems where 64-bit atomics may require special handling
- Complements the 32-bit atomic tests to ensure full coverage of PostgreSQL's atomic operation capabilities