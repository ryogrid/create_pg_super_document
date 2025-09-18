# pg_atomic_test_set_flag

## Location
[src/include/port/atomics.h:178-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L178-L190)

## Overview
Atomically tests and sets an atomic flag, implementing a Test-And-Set (TAS) operation with acquire memory barrier semantics.

## Definition
```c
static inline bool pg_atomic_test_set_flag(volatile pg_atomic_flag *ptr)
```

## Detailed Description
The `pg_atomic_test_set_flag` function implements the classic Test-And-Set (TAS) atomic operation on a PostgreSQL atomic flag. It atomically checks the current value of the flag and sets it to true, returning whether the operation successfully acquired the flag (i.e., whether the flag was previously false).

This function provides acquire semantics with a read barrier, ensuring that subsequent memory operations cannot be reordered before this operation completes. This makes it suitable for implementing locks and other synchronization primitives where memory ordering guarantees are critical.

The function returns true if the flag was successfully set (meaning it was previously false), and false if the flag was already set by another thread or process.

## Parameters / Member Variables
- `ptr`: Pointer to the volatile `pg_atomic_flag` structure to test and set. The volatile qualifier ensures proper memory access behavior in concurrent environments.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_test_set_flag_impl](pg_atomic_test_set_flag_impl.md)
- Structures referenced:
  - [pg_atomic_flag](pg_atomic_flag.md)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (src/backend/postmaster/autovacuum.c:2399, 2507)
  - [test_atomic_flag](../t/test_atomic_flag.md) (src/test/regress/regress.c:718, 720, 723)

## Notes and Other Information
- This function implements the Test-And-Set atomic primitive, a fundamental building block for lock-free programming
- Provides acquire semantics with read barrier, ensuring proper memory ordering in multi-threaded environments
- Return value of true indicates successful acquisition (flag was previously false)
- Return value of false indicates the flag was already set by another thread
- The function is implemented as a static inline wrapper for performance optimization
- Commonly used in implementing spinlocks, mutexes, and other synchronization mechanisms
- The underlying implementation uses platform-specific atomic operations or spinlock protection depending on hardware capabilities