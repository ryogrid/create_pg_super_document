# s_init_lock_sema

## Location
[src/backend/storage/lmgr/spin.c:121-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/spin.c#L121-L151)

## Overview
Initializes a spinlock by assigning it a semaphore index from the available pool, with support for nested atomic operations.

## Definition
```c
void s_init_lock_sema(volatile slock_t *lock, bool nested)
```

## Detailed Description
This function initializes a spinlock in the semaphore-based emulation system by assigning it a unique semaphore index. It maintains a round-robin counter to distribute spinlocks across available semaphores to minimize contention. The function supports two modes: regular spinlocks and nested atomic operations. When nested is true, it allocates from a separate pool of semaphores dedicated to atomic operations that can be safely nested inside spinlock-protected sections.

## Parameters / Member Variables
- `lock`: Pointer to the spinlock variable to initialize
- `nested`: Boolean flag indicating whether this is for nested atomic operations (true) or regular spinlocks (false)

## Dependencies
- Functions called/Symbols referenced:
  - slock_t (spinlock type definition)
  - NUM_SPINLOCK_SEMAPHORES (number of semaphores for regular spinlocks)
  - NUM_ATOMICS_SEMAPHORES (number of semaphores for atomic operations)
  - s_check_valid() (validates the assigned semaphore index)
- Called from:
  - pg_atomic_init_flag_impl (in src/backend/port/atomics.c:67)
  - pg_atomic_init_u32_impl (in src/backend/port/atomics.c:116)
  - pg_atomic_init_u64_impl (in src/backend/port/atomics.c:192)
  - slock_t (in src/include/storage/s_lock.h:739)
  - S_INIT_LOCK (in src/include/storage/s_lock.h:744)

## Notes and Other Information
- Uses a static counter for round-robin distribution of semaphores to minimize contention
- Separates regular spinlocks and atomic operation semaphores to avoid deadlocks when atomics are nested inside spinlocks
- Regular spinlocks use indices 1 to NUM_SPINLOCK_SEMAPHORES
- Nested atomic operations use indices (1 + NUM_SPINLOCK_SEMAPHORES) to (NUM_SPINLOCK_SEMAPHORES + NUM_ATOMICS_SEMAPHORES)
- The assigned index is validated using s_check_valid() before assignment
- Part of the spinlock emulation infrastructure that provides safe concurrency primitives on systems without hardware spinlock support