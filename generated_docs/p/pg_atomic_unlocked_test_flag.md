# pg_atomic_unlocked_test_flag

## Location
src/include/port/atomics.h: 191 - 201

## Overview
Tests whether an atomic flag is currently unlocked (not set) without modifying its state and without memory barrier semantics.

## Definition
```c
static inline bool pg_atomic_unlocked_test_flag(volatile pg_atomic_flag *ptr)
```

## Detailed Description
The `pg_atomic_unlocked_test_flag` function provides a non-blocking way to check if an atomic flag is currently in an unlocked state (value is false/0) without attempting to modify it. This is useful for polling operations where you want to check lock availability before attempting to acquire it, or for implementing lock-free algorithms that need to inspect lock state.

Unlike `pg_atomic_test_set_flag`, this function does not modify the flag state and provides no memory barrier semantics. This makes it suitable for optimistic checks where strict memory ordering is not required, such as when deciding whether to attempt lock acquisition or when implementing backoff strategies in busy-wait loops.

The function returns true if the flag is unlocked (available), and false if the flag is currently set (locked).

## Parameters / Member Variables
- `ptr`: Pointer to the volatile `pg_atomic_flag` structure to test. The volatile qualifier ensures the compiler will not optimize away repeated accesses to this memory location.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_unlocked_test_flag_impl](pg_atomic_unlocked_test_flag_impl.md)
- Structures referenced:
  - [pg_atomic_flag](pg_atomic_flag.md)
- Called from (representative examples):
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md) (src/backend/postmaster/autovacuum.c:1690)
  - [AutoVacuumUpdateCostLimit](../A/AutoVacuumUpdateCostLimit.md) (src/backend/postmaster/autovacuum.c:1728)
  - [autovac_recalculate_workers_for_balance](../a/autovac_recalculate_workers_for_balance.md) (src/backend/postmaster/autovacuum.c:1768)
  - [test_atomic_flag](../t/test_atomic_flag.md) (src/test/regress/regress.c:717, 719, 722)

## Notes and Other Information
- This function provides no memory barrier semantics, making it suitable for optimistic polling operations
- Returns true when the flag is unlocked (available for acquisition)
- Returns false when the flag is currently locked (set by another thread)
- Does not modify the flag state, making it safe to call repeatedly without side effects
- Commonly used in busy-wait loops with backoff strategies to reduce contention
- The lack of barrier semantics means this should not be used where strict memory ordering is required
- Useful for implementing optimistic lock acquisition patterns and reducing unnecessary contention in high-concurrency scenarios