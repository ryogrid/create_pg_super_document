# pg_atomic_fetch_and_u32

## Location
[src/include/port/atomics.h:391-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L391-L404)

## Overview
Atomically performs a bitwise AND operation between a 32-bit unsigned atomic variable and a given value, returning the original value before the operation.

## Definition
```c
static inline uint32
pg_atomic_fetch_and_u32(volatile pg_atomic_uint32 *ptr, uint32 and_)
```

## Detailed Description
This function performs an atomic fetch-and-AND operation on a 32-bit unsigned integer. It atomically applies a bitwise AND operation between the value pointed to by `ptr` and the value `and_`, storing the result back to the location pointed to by `ptr`, and returns the original value that was stored at that location before the operation.

The operation provides full memory barrier semantics, ensuring that all memory operations before this call are completed before the bitwise AND operation, and all memory operations after this call happen after the operation. This function is particularly useful for atomically clearing specific bits in a value while preserving others, which is common in flag management and bit manipulation scenarios.

The function is implemented as a wrapper around `pg_atomic_fetch_and_u32_impl`, which contains the platform-specific implementation.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `and_`: The 32-bit unsigned integer value to AND with the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](pg_atomic_uint32.md) (type definition)
  - AssertPointerAlignment (alignment check)
  - [pg_atomic_fetch_and_u32_impl](pg_atomic_fetch_and_u32_impl.md) (platform-specific implementation)
- Called from (representative examples):
  - [LWLockWaitListUnlock](../L/LWLockWaitListUnlock.md) (src/backend/storage/lmgr/lwlock.c:913)
  - [LWLockDequeueSelf](../L/LWLockDequeueSelf.md) (src/backend/storage/lmgr/lwlock.c:1107)
  - [test_atomic_uint32](../t/test_atomic_uint32.md) (src/test/regress/regress.c:793-796)

## Notes and Other Information
- Returns the value that was stored before the bitwise AND operation
- Provides full barrier semantics, making it suitable for synchronization and coordination
- The pointer must be 4-byte aligned as enforced by AssertPointerAlignment
- Commonly used for atomically clearing bits in flag variables and status registers
- Particularly useful in lock management where specific status bits need to be cleared atomically
- The operation follows standard bitwise AND semantics: result bit is 1 only if both operand bits are 1
- Often used in conjunction with other atomic operations to implement complex state management
- Essential for implementing atomic bit manipulation in concurrent data structures

## Simplified Source

```c
// Simplified version of pg_atomic_fetch_and_u32
static inline uint32
pg_atomic_fetch_and_u32(volatile pg_atomic_uint32 *ptr, uint32 and_)
{
    // Ensure pointer is properly aligned for atomic operations
    AssertPointerAlignment(ptr, 4);

    // Delegate to platform-specific implementation
    // Atomically: old_value = *ptr; *ptr = *ptr & and_; return old_value;
    return pg_atomic_fetch_and_u32_impl(ptr, and_);
}
```

Key simplifications made:
- Preserved the essential wrapper function structure
- Kept the alignment assertion as it's critical for correctness
- Added inline comment explaining the atomic operation semantics
- Maintained the delegation to platform-specific implementation
- Function is already quite simple, so minimal simplification was needed