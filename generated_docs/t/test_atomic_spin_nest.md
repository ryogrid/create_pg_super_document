# test_atomic_spin_nest

## Location
src/test/regress/regress.c: 964 - 966

## Overview
test_atomic_spin_nest is a static test function that verifies atomic operations can be safely performed while holding a spinlock in PostgreSQL.

## Definition


## Detailed Description
This function tests the compatibility between atomic operations and spinlocks in PostgreSQL. It verifies that atomic operations can be performed safely while holding a spinlock, which is particularly important when both --disable-spinlocks and --disable-atomics compilation options are used. The test works by initializing multiple atomic variables (more than NUM_SPINLOCK_SEMAPHORES) and then manipulating them while holding a spinlock to detect any potential conflicts or overlaps in the underlying implementation.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md) (spinlock type)
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (32-bit atomic variable type)
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md) (64-bit atomic variable type)
  - SpinLockInit (spinlock initialization)
  - SpinLockAcquire (acquire spinlock)
  - SpinLockRelease (release spinlock)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md)/pg_atomic_init_u64 (atomic initialization)
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md)/pg_atomic_fetch_add_u64 (atomic add operations)
  - [pg_atomic_fetch_sub_u32](../p/pg_atomic_fetch_sub_u32.md)/pg_atomic_fetch_sub_u64 (atomic subtract operations)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)/pg_atomic_read_u64 (atomic read operations)
  - EXPECT_EQ_U32/EXPECT_EQ_U64 (test assertion macros)
- Called from (representative examples):
  - [test_atomic_ops](test_atomic_ops.md) (main atomic operations test function)

## Notes and Other Information
- Located in src/test/regress/regress.c:964-966
- Uses NUM_TEST_ATOMICS arrays for comprehensive testing
- The test is designed to be cheap enough to run always, not just in specific build configurations
- Tests both 32-bit and 64-bit atomic operations
- Critical for ensuring thread safety in PostgreSQL's locking mechanisms
- Part of PostgreSQL's regression test suite for atomic operations