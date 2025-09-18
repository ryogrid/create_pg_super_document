# pg_atomic_sub_fetch_u32

## Location
[src/include/port/atomics.h:434-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L434-L447)

## Overview
Atomically subtracts a value from a 32-bit unsigned integer variable and returns the new value after the subtraction operation.

## Definition
```c
static inline uint32
pg_atomic_sub_fetch_u32(volatile pg_atomic_uint32 *ptr, int32 sub_)
```

## Detailed Description
This function provides an atomic subtract-and-fetch operation on a 32-bit unsigned integer. It atomically subtracts the value `sub_` from the value stored at the memory location pointed to by `ptr`, then returns the new value after the subtraction. The function guarantees full memory barrier semantics, ensuring proper ordering of memory operations across threads.

The function serves as a high-level wrapper around the platform-specific implementation `pg_atomic_sub_fetch_u32_impl`, providing a consistent interface across different architectures while ensuring proper pointer alignment. The function includes an assertion to prevent the use of INT_MIN as the subtraction value due to platform limitations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `sub_`: The signed 32-bit value to be subtracted from the variable (must not be INT_MIN)

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment
  - Assert
  - [pg_atomic_sub_fetch_u32_impl](pg_atomic_sub_fetch_u32_impl.md)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (type)
- Called from (representative examples):
  - [compute_parallel_delay](../c/compute_parallel_delay.md)
  - [parallel_vacuum_process_safe_indexes](parallel_vacuum_process_safe_indexes.md)
  - [parallel_vacuum_process_unsafe_indexes](parallel_vacuum_process_unsafe_indexes.md)
  - [tbm_free_shared_area](../t/tbm_free_shared_area.md)
  - [pa_decr_and_wait_stream_block](pa_decr_and_wait_stream_block.md)
  - LWLockRelease
  - pgstat_drop_entry_internal

## Notes and Other Information
- The function enforces 4-byte alignment for the pointer parameter through AssertPointerAlignment
- Includes an assertion to prevent `sub_` from being INT_MIN due to platform-specific limitations
- Provides full barrier semantics, making it suitable for synchronization primitives
- Commonly used in PostgreSQL's parallel processing, locking mechanisms, and memory management
- Used extensively in vacuum operations, bitmap operations, lightweight locks, and statistics management
- The inline nature of the function ensures minimal overhead while maintaining atomicity guarantees
- Platform limitations prevent the use of INT_MIN as the subtraction value due to potential overflow issues in two's complement arithmetic