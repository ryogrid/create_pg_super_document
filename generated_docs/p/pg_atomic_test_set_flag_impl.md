# pg_atomic_test_set_flag_impl

## Location
[src/backend/port/atomics.c:76-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/atomics.c#L76-L88)

## Overview
Atomically tests the current value of a flag and sets it to true, returning whether the flag was previously unset (available).

## Definition

```c
bool
pg_atomic_test_set_flag_impl(volatile pg_atomic_flag *ptr)
```
## Detailed Description
pg_atomic_test_set_flag_impl is a fallback implementation of the atomic test-and-set operation for flags on platforms that lack native atomic flag support. It is compiled only when PG_HAVE_ATOMIC_FLAG_SIMULATION is defined. The function performs an atomic test-and-set operation using spinlock protection: it acquires the spinlock, reads the current flag value, sets the flag to true, releases the spinlock, and returns whether the flag was previously false (indicating successful acquisition). This provides the classic test-and-set semantics used for implementing higher-level synchronization primitives.

## Parameters / Member Variables
- : Pointer to the pg_atomic_flag structure to test and set

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (acquire spinlock protection)
  - SpinLockRelease (release spinlock protection)
  - [pg_atomic_flag](pg_atomic_flag.md) (structure type)
  - [slock_t](../s/slock_t.md) (spinlock type)
  - uint32 (for storing old value)
- Called from (representative examples):
  - [pg_atomic_test_set_flag](pg_atomic_test_set_flag.md) (via atomic operations framework)

## Notes and Other Information
- This is a fallback implementation only used when native atomic flags are unavailable
- Returns true if the flag was previously false (successful acquisition), false if already set
- Uses spinlock protection to ensure atomicity of the test-and-set operation
- The comparison  converts the boolean flag value to the expected return semantics
- Part of PostgreSQL's atomic flag simulation framework alongside pg_atomic_init_flag_impl and pg_atomic_clear_flag_impl
- Defined in src/backend/port/atomics.c under conditional compilation (PG_HAVE_ATOMIC_FLAG_SIMULATION)
- Critical section is kept minimal (just the read-modify-write operation) for performance

## Simplified Source

```c
bool pg_atomic_test_set_flag_impl(volatile pg_atomic_flag *ptr) {
    uint32 oldval;

    // Acquire spinlock for atomic operation
    SpinLockAcquire((slock_t *) &ptr->sema);

    // Test current value and set to true
    oldval = ptr->value;
    ptr->value = true;

    // Release spinlock
    SpinLockRelease((slock_t *) &ptr->sema);

    // Return true if flag was previously unset (available)
    return oldval == 0;
}
```