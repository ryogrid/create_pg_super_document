# pg_atomic_init_u64_impl

## Location
[src/backend/port/atomics.c:182-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/atomics.c#L182-L199)

## Overview
Initializes a 64-bit atomic unsigned integer variable with a specified value, setting up both the synchronization mechanism and the initial value.

## Definition


## Detailed Description
This function provides the implementation for initializing 64-bit atomic variables in PostgreSQL's fallback atomic operations system. It serves as the backend implementation when hardware-specific atomic operations are not available. The function performs two critical tasks: initializing the synchronization primitive (either a semaphore or spinlock depending on system capabilities) and setting the initial value of the atomic variable.

The function includes compile-time assertions to ensure proper size alignment between the atomic structure's semaphore field and the system's spinlock type. It conditionally uses either semaphore-based or spinlock-based synchronization depending on whether the system has native spinlock support.

## Parameters / Member Variables
- : Pointer to the volatile pg_atomic_uint64 structure to be initialized
- : The initial 64-bit unsigned integer value to store in the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - StaticAssertDecl (compile-time assertion)
  - s_init_lock_sema (semaphore initialization when spinlocks unavailable)
  - SpinLockInit (spinlock initialization when spinlocks available)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (atomic structure type)
  - [slock_t](../s/slock_t.md) (spinlock type)
- Called from (representative examples):
  - [pg_atomic_init_u64](pg_atomic_init_u64.md) (public API wrapper)
  - PG_HAVE_ATOMIC_INIT_U64 (feature availability macro)

## Notes and Other Information
- This is part of PostgreSQL's fallback atomic operations implementation used when hardware atomic operations are unavailable
- The function handles both semaphore-based and spinlock-based synchronization mechanisms
- Includes safety checks for nested atomic usage when spinlocks are held
- The implementation ensures thread-safe initialization of the atomic variable
- Located in src/backend/port/atomics.c as part of the portability layer