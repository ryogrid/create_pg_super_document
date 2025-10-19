# pg_atomic_sub_fetch_u64_impl

## Location
[src/include/port/atomics/generic.h:415-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L415-L421)

## Overview
Performs an atomic subtract-and-fetch operation on a 64-bit unsigned integer, returning the new value after the subtraction operation is complete.

## Definition

```c
static inline uint64
pg_atomic_sub_fetch_u64_impl(volatile pg_atomic_uint64 *ptr, int64 sub_)
```
## Detailed Description
This function implements the atomic subtract-and-fetch operation for 64-bit unsigned integers by leveraging the existing  function and subtracting the decrement value from its result. Unlike fetch-and-subtract which returns the original value, this function returns the new value after the subtraction has been performed. This is implemented as a generic fallback when direct subtract-and-fetch atomic operations are not available in hardware.

The implementation follows the pattern: new_value = fetch_sub(ptr, sub_) - sub_, ensuring atomicity through the underlying fetch-sub operation while providing the post-operation semantics.

## Parameters / Member Variables
- `*ptr`: Pointer to the atomic 64-bit unsigned integer variable to be modified
- `sub_`: The signed 64-bit value to be subtracted from the current value
## Dependencies
- Functions called/Symbols referenced:
  - 
  -  (type)
  -  (conditional compilation)
  -  (conditional compilation)
- Called from (representative examples):
  - 

## Notes and Other Information
- This is a generic implementation that builds upon fetch-sub primitives
- Located in the generic.h header, used when platform-specific optimizations are not available
- The function accepts a signed integer parameter for the subtraction value
- Thread-safe operation guaranteed by the underlying atomic fetch-sub implementation
- The implementation ensures that the subtraction and result calculation appear atomic to other threads
- Part of PostgreSQL's portable atomic operations framework for cross-platform compatibility
- Complementary to , providing the subtract equivalent functionality

## Simplified Source

```c
static inline uint64
pg_atomic_sub_fetch_u64_impl(volatile pg_atomic_uint64 *ptr, int64 sub_)
{
    // Atomic subtract-and-fetch: get new value after subtraction
    return pg_atomic_fetch_sub_u64_impl(ptr, sub_) - sub_;
}
```

**Key Points:**
- Atomically subtracts `sub_` from the value at `ptr`
- Returns the new value after subtraction (not the original value)
- Built on top of fetch-sub operation for portability
- Thread-safe atomic operation