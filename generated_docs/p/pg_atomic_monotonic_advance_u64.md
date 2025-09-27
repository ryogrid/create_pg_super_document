# pg_atomic_monotonic_advance_u64

## Location
[src/include/port/atomics.h:580-603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L580-L603)

## Overview
Atomically advances a 64-bit unsigned integer to at least a specified target value using only atomic operations, ensuring monotonic progression.

## Definition
```c
static inline uint64
pg_atomic_monotonic_advance_u64(volatile pg_atomic_uint64 *ptr, uint64 target)
```

## Detailed Description
This function ensures that a 64-bit atomic variable is monotonically advanced to at least the specified target value. It uses a compare-and-swap loop to safely update the value without allowing it to decrease. The function provides full memory barrier semantics regardless of whether the value is actually changed.

The implementation first reads the current value atomically. If it's already at or above the target, it issues a memory barrier and returns the current value. Otherwise, it enters a loop using compare-and-exchange operations to attempt to set the value to the target. The loop continues until either the target is successfully set or another thread advances the value to at least the target.

This function is particularly useful for scenarios where multiple threads need to ensure a shared counter or sequence number progresses forward but should never go backward, such as in WAL (Write-Ahead Logging) operations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to be advanced
- `target`: The minimum value that the atomic variable should reach

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u64_impl](pg_atomic_read_u64_impl.md)
  - [pg_atomic_compare_exchange_u64](pg_atomic_compare_exchange_u64.md)
  - pg_memory_barrier
  - AssertPointerAlignment
  - PG_HAVE_ATOMIC_U64_SIMULATION (conditional compilation)
- Called from (representative examples):
  - [WaitXLogInsertionsToFinish](../W/WaitXLogInsertionsToFinish.md)

## Notes and Other Information
- Provides full barrier semantics even when the value is unchanged
- The function requires 8-byte alignment of the target pointer when not using simulation mode
- Uses a compare-and-swap loop to handle concurrent modifications by other threads
- Guarantees monotonic advancement - the value can only increase, never decrease
- Returns the final observed value, which may be higher than the target if another thread advanced it further
- This is commonly used in PostgreSQL's WAL system for coordinating write operations across multiple processes

## Simplified Source

```c
// Simplified version of pg_atomic_monotonic_advance_u64
static inline uint64 pg_atomic_monotonic_advance_u64(volatile pg_atomic_uint64 *ptr, uint64 target) {
    uint64 current_value;

    // Verify alignment when not using simulation mode
    #ifndef PG_HAVE_ATOMIC_U64_SIMULATION
    AssertPointerAlignment(ptr, 8);
    #endif

    // Read current value atomically
    current_value = pg_atomic_read_u64_impl(ptr);

    // If already at or above target, return with memory barrier
    if (current_value >= target) {
        pg_memory_barrier();
        return current_value;
    }

    // Loop until we reach the target or another thread does
    while (current_value < target) {
        // Try to set value to target using compare-and-exchange
        if (pg_atomic_compare_exchange_u64(ptr, &current_value, target)) {
            return target;  // Successfully set to target
        }
        // current_value was updated by compare_exchange to the actual value
    }

    return current_value;
}
```

Key simplifications made:
- Added comments explaining the monotonic advancement logic
- Clarified the compare-and-exchange loop behavior
- Explained how current_value gets updated by the failed compare-exchange
- Maintained all essential atomic operations and memory barriers
- Preserved the alignment checks and simulation mode handling
- Simplified variable naming for clarity