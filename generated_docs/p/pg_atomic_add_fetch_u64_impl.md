# pg_atomic_add_fetch_u64_impl

## Location
[src/include/port/atomics/generic.h:406-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L406-L412)

## Overview
Performs an atomic add-and-fetch operation on a 64-bit unsigned integer, returning the new value after the addition operation is complete.

## Definition


## Detailed Description
This function implements the atomic add-and-fetch operation for 64-bit unsigned integers by leveraging the existing  function and adding the increment value to its result. Unlike fetch-and-add which returns the original value, this function returns the new value after the addition has been performed. This is implemented as a generic fallback when direct add-and-fetch atomic operations are not available in hardware.

The implementation follows the pattern: new_value = fetch_add(ptr, add_) + add_, ensuring atomicity through the underlying fetch-add operation while providing the post-operation semantics.

## Parameters / Member Variables
- : Pointer to the atomic 64-bit unsigned integer variable to be modified
- : The signed 64-bit value to be added to the current value (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - 
  -  (type)
  -  (conditional compilation)
  -  (conditional compilation)
- Called from (representative examples):
  - 

## Notes and Other Information
- This is a generic implementation that builds upon fetch-add primitives
- Located in the generic.h header, used when platform-specific optimizations are not available
- The function accepts a signed integer parameter, allowing both addition and subtraction operations
- Thread-safe operation guaranteed by the underlying atomic fetch-add implementation
- The implementation ensures that the addition and result calculation appear atomic to other threads
- Part of PostgreSQL's portable atomic operations framework for cross-platform compatibility