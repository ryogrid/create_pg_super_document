# test_atomic_uint32

## Location
[src/test/regress/regress.c:728-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L728-L799)

## Overview
A comprehensive static test function that validates the complete functionality of PostgreSQL's 32-bit atomic unsigned integer operations, including arithmetic, comparison, exchange, and bitwise operations.

## Definition

```c
static void
test_atomic_uint32(void)
```
## Detailed Description
The  function is an extensive unit test that systematically validates all atomic operations available for 32-bit unsigned integers in PostgreSQL. The function tests initialization, read/write operations, atomic arithmetic (add, subtract with both fetch-then-op and op-then-fetch variants), atomic exchange, compare-and-swap operations, and bitwise operations (AND, OR). It includes comprehensive edge case testing around numerical limits (INT_MAX, UINT_MAX, PG_INT16_MAX/MIN) to ensure proper handling of overflow and underflow conditions. The function also tests the compare-and-exchange operation with a retry loop to handle potential spurious failures due to system interrupts, which is a realistic scenario in concurrent environments.

## Parameters / Member Variables
- No parameters: void function with no arguments
- Local variables:
  - : pg_atomic_uint32 type used for testing atomic operations
  - : uint32 used for compare-exchange operations
  - : int loop counter for retry logic

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md): Type definition for 32-bit atomic unsigned integer
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md): Initializes atomic uint32 variable
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md): Atomically reads current value
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md): Atomically writes new value
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md): Atomically adds and returns old value
  - [pg_atomic_add_fetch_u32](../p/pg_atomic_add_fetch_u32.md): Atomically adds and returns new value
  - [pg_atomic_fetch_sub_u32](../p/pg_atomic_fetch_sub_u32.md): Atomically subtracts and returns old value
  - [pg_atomic_sub_fetch_u32](../p/pg_atomic_sub_fetch_u32.md): Atomically subtracts and returns new value
  - [pg_atomic_exchange_u32](../p/pg_atomic_exchange_u32.md): Atomically exchanges values
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md): Atomic compare-and-swap operation
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md): Atomic bitwise OR with fetch
  - [pg_atomic_fetch_and_u32](../p/pg_atomic_fetch_and_u32.md): Atomic bitwise AND with fetch
  - EXPECT_EQ_U32: Test assertion macro for uint32 equality
  - EXPECT_TRUE: Test assertion macro for boolean conditions
  - Constants: INT_MAX, UINT_MAX, PG_INT16_MAX, PG_INT16_MIN
- Called from (representative examples):
  - [test_atomic_ops](test_atomic_ops.md): Main atomic operations test function

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Part of PostgreSQL's regression testing framework for atomic operations
- Tests exhaustive scenarios including:
  1. Basic initialization, read, and write operations
  2. Atomic arithmetic with both pre- and post-operation value returns
  3. Boundary value testing at numerical limits to verify overflow handling
  4. Compare-and-swap operations with both successful and failed attempts
  5. Bitwise operations for flag manipulation
- Includes a retry loop (up to 1000 attempts) for compare-exchange to handle spurious failures
- Tests wrap-around behavior at UINT_MAX boundary
- Validates that failed compare-exchange operations leave the expected value updated with the actual value
- Located in src/test/regress/regress.c as part of PostgreSQL's test infrastructure
- Critical for ensuring atomic operations work correctly across different CPU architectures and compiler optimizations
- The comprehensive nature of this test helps catch subtle concurrency bugs that might only appear under specific conditions

## Simplified Source

```c
static void test_atomic_uint32(void) {
    pg_atomic_uint32 var;
    uint32 expected;
    int i;

    // Basic read/write operations
    pg_atomic_init_u32(&var, 0);
    EXPECT_EQ_U32(pg_atomic_read_u32(&var), 0);
    pg_atomic_write_u32(&var, 3);
    EXPECT_EQ_U32(pg_atomic_read_u32(&var), 3);

    // Arithmetic operations (add/subtract with fetch variations)
    EXPECT_EQ_U32(pg_atomic_fetch_add_u32(&var, 1), 3);  // Returns old value
    EXPECT_EQ_U32(pg_atomic_fetch_sub_u32(&var, 1), 4);  // Returns old value
    EXPECT_EQ_U32(pg_atomic_sub_fetch_u32(&var, 3), 0);  // Returns new value
    EXPECT_EQ_U32(pg_atomic_add_fetch_u32(&var, 10), 10); // Returns new value

    // Exchange operations
    EXPECT_EQ_U32(pg_atomic_exchange_u32(&var, 5), 10);
    EXPECT_EQ_U32(pg_atomic_exchange_u32(&var, 0), 5);

    // Test numerical boundary conditions (overflow/underflow)
    pg_atomic_fetch_add_u32(&var, INT_MAX);
    pg_atomic_fetch_add_u32(&var, INT_MAX);
    // ... additional boundary tests ...

    // Compare-and-swap with retry loop for spurious failures
    for (i = 0; i < 1000; i++) {
        expected = 0;
        if (!pg_atomic_compare_exchange_u32(&var, &expected, 1))
            break;
    }
    if (i == 1000)
        elog(ERROR, "atomic_compare_exchange_u32() never succeeded");

    // Bitwise operations (OR/AND for flag manipulation)
    pg_atomic_write_u32(&var, 0);
    EXPECT_TRUE(!(pg_atomic_fetch_or_u32(&var, 1) & 1));   // Set bit 0
    EXPECT_TRUE(pg_atomic_fetch_or_u32(&var, 2) & 1);      // Set bit 1
    EXPECT_EQ_U32(pg_atomic_fetch_and_u32(&var, ~2), 3);   // Clear bit 1
    EXPECT_EQ_U32(pg_atomic_fetch_and_u32(&var, ~1), 1);   // Clear bit 0
}
```