# pg_atomic_write_u64_impl

## Location
[src/include/port/atomics/generic.h:284-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L284-L298)

## Overview
Atomically writes a 64-bit unsigned integer value to a memory location, providing thread-safe assignment for 64-bit values on platforms where aligned 64-bit writes are guaranteed to be atomic.

## Definition


## Detailed Description
This function implements the platform-specific atomic write operation for 64-bit unsigned integers in PostgreSQL's atomic operations framework. It performs a direct assignment to the atomic variable's value field, relying on the platform's guarantee that aligned 64-bit memory writes are atomic operations. The function includes an assertion to verify proper 8-byte alignment of the target pointer, which is crucial for the atomicity guarantee. This implementation is part of the generic atomics header and is used when the platform supports native atomic 64-bit writes without requiring special CPU instructions or compiler intrinsics.

## Parameters / Member Variables
- : Pointer to the atomic 64-bit unsigned integer variable that will receive the new value
- : The 64-bit unsigned integer value to be written atomically to the target location

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (ensures 8-byte alignment)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (the atomic 64-bit integer type)
- Called from (representative examples):
  - [pg_atomic_write_u64](pg_atomic_write_u64.md) (public interface wrapper)

## Notes and Other Information
- This implementation assumes the platform guarantees atomic 64-bit aligned writes
- The 8-byte alignment assertion is critical for correctness and will cause debug builds to fail if violated
- This is part of PostgreSQL's cross-platform atomic operations abstraction layer
- On platforms without native 64-bit atomic write support, a different implementation would be selected