# pg_atomic_flag

## Location
src/include/port/atomics/arch-x86.h: 57 - 60

## Overview
A lightweight atomic flag structure providing lock-free synchronization primitives in PostgreSQL for implementing test-and-set operations and basic spinlocks.

## Definition


## Detailed Description
The  struct is PostgreSQL's foundational atomic synchronization primitive, designed to provide lock-free test-and-set functionality across different hardware architectures. It serves as the building block for implementing spinlocks and other low-level synchronization mechanisms within the PostgreSQL backend.

The structure contains a single volatile character field that can atomically transition between set (1) and unset (0) states. This simplicity ensures maximum portability while providing the essential compare-and-swap semantics needed for lock-free programming. The volatile qualifier prevents compiler optimizations that might interfere with the atomic semantics.

PostgreSQL's atomic flag implementation is architecture-aware, with platform-specific optimizations available for x86, ARM, and other architectures, while falling back to generic implementations using higher-level atomic operations or even spinlocks when native atomic support is unavailable.

## Parameters / Member Variables
- : A volatile character field that stores the flag state (0 for unset, non-zero for set). The volatile qualifier ensures the compiler doesn't optimize away memory accesses to this field.

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure definition)
- Called from (representative examples):
  - [pg_atomic_init_flag_impl](pg_atomic_init_flag_impl.md)
  - [pg_atomic_test_set_flag_impl](pg_atomic_test_set_flag_impl.md)  
  - [pg_atomic_clear_flag_impl](pg_atomic_clear_flag_impl.md)
  - [pg_atomic_unlocked_test_flag_impl](pg_atomic_unlocked_test_flag_impl.md)
  - [WorkerInfoData](../W/WorkerInfoData.md) (autovacuum worker synchronization)

## Notes and Other Information
- The flag is typically used through the atomic operation wrappers (pg_atomic_init_flag, pg_atomic_test_set_flag, pg_atomic_clear_flag, pg_atomic_unlocked_test_flag) rather than direct manipulation
- Provides acquire semantics on test-and-set operations and release semantics on clear operations for proper memory ordering
- Used extensively in PostgreSQL's autovacuum system for worker coordination
- The actual atomic operations are implemented differently across architectures but provide consistent semantics
- Part of PostgreSQL's broader atomic operations framework introduced to reduce dependence on heavyweight locking mechanisms