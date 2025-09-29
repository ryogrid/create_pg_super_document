# pg_atomic_init_u32_impl

## Location
[src/backend/port/atomics.c:106-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/atomics.c#L106-L123)

## Overview
Implementation function that initializes a 32-bit atomic unsigned integer variable with a specified value and sets up its synchronization mechanism.

## Definition
```c
void pg_atomic_init_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 val_)
```

## Detailed Description
This function provides the backend implementation for initializing a 32-bit atomic unsigned integer. It performs two critical operations: initializing the synchronization primitive (spinlock or semaphore) that protects the atomic variable, and setting the initial value. The function uses conditional compilation to choose between spinlocks and semaphores based on platform capabilities.

The function includes a static assertion to ensure that the semaphore field in the atomic structure is large enough to hold a spinlock. This is part of PostgreSQL's fallback atomic operations implementation used when native atomic operations are not available.

## Parameters / Member Variables
- `ptr`: Volatile pointer to a pg_atomic_uint32 structure to be initialized
- `val_`: Initial 32-bit unsigned integer value to assign to the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](pg_atomic_uint32.md) (structure type)
  - StaticAssertDecl (compile-time assertion macro)
  - [slock_t](../s/slock_t.md) (spinlock type)
  - [s_init_lock_sema](../s/s_init_lock_sema.md) (semaphore-based lock initialization)
  - SpinLockInit (spinlock initialization)
- Called from (representative examples):
  - [pg_atomic_init_u32](pg_atomic_init_u32.md) (wrapper function in atomics.h)
  - PG_HAVE_ATOMIC_INIT_U32 (feature detection macro)

## Notes and Other Information
- Uses compile-time assertions to verify structure layout compatibility
- Conditionally uses either spinlocks (HAVE_SPINLOCKS) or semaphores for synchronization
- The semaphore-based path includes special handling for nested atomic usage
- Part of the fallback implementation when hardware atomic operations are unavailable
- Ensures proper initialization of both the synchronization mechanism and the value

## Simplified Source

```c
void
pg_atomic_init_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 val_)
{
    // Compile-time check: ensure semaphore field can hold a spinlock
    StaticAssertDecl(sizeof(ptr->sema) >= sizeof(slock_t),
                     "size mismatch of atomic_uint32 vs slock_t");

#ifndef HAVE_SPINLOCKS
    // On platforms without spinlocks, use semaphore-based synchronization
    // Special handling for nested atomic usage while spinlock is held
    s_init_lock_sema((slock_t *) &ptr->sema, true);
#else
    // On platforms with spinlocks, use standard spinlock initialization
    SpinLockInit((slock_t *) &ptr->sema);
#endif

    // Set the initial value
    ptr->value = val_;
}
```