# pg_atomic_add_fetch_u32

## Location
src/include/port/atomics.h: 419 - 433

## Overview
Atomically adds a value to a 32-bit unsigned integer variable and returns the new value after the addition operation.

## Definition
```c
static inline uint32
pg_atomic_add_fetch_u32(volatile pg_atomic_uint32 *ptr, int32 add_)
```

## Detailed Description
This function provides an atomic add-and-fetch operation on a 32-bit unsigned integer. It atomically adds the value `add_` to the value stored at the memory location pointed to by `ptr`, then returns the new value after the addition. The function guarantees full memory barrier semantics, ensuring proper ordering of memory operations across threads.

The function serves as a high-level wrapper around the platform-specific implementation `pg_atomic_add_fetch_u32_impl`, providing a consistent interface across different architectures while ensuring proper pointer alignment. Note that the `add_` parameter is signed, allowing for both addition (positive values) and subtraction (negative values).

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `add_`: The signed 32-bit value to be added to the variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment
  - [pg_atomic_add_fetch_u32_impl](pg_atomic_add_fetch_u32_impl.md)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (type)
- Called from (representative examples):
  - [compute_parallel_delay](../c/compute_parallel_delay.md)
  - [parallel_vacuum_process_safe_indexes](parallel_vacuum_process_safe_indexes.md)
  - [parallel_vacuum_process_unsafe_indexes](parallel_vacuum_process_unsafe_indexes.md)
  - [tbm_prepare_shared_iterate](../t/tbm_prepare_shared_iterate.md)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)

## Notes and Other Information
- The function enforces 4-byte alignment for the pointer parameter through AssertPointerAlignment
- Provides full barrier semantics, making it suitable for synchronization primitives
- Commonly used in PostgreSQL's parallel processing and replication mechanisms
- The signed `add_` parameter allows for both increment and decrement operations
- Used extensively in vacuum operations, bitmap operations, and logical replication
- The inline nature of the function ensures minimal overhead while maintaining atomicity guarantees