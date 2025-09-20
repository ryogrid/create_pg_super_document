# add_size

## Location
[src/backend/storage/ipc/shmem.c:493-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L493-L509)

## Overview
add_size safely adds two Size values while checking for overflow, preventing integer overflow vulnerabilities in shared memory size calculations.

## Definition

```c
Size
add_size(Size s1, Size s2)
```
## Detailed Description
This function performs overflow-safe addition of two Size values, which is crucial for shared memory calculations where integer overflow could lead to undersized allocations and memory corruption. The function implements a simple but effective overflow detection mechanism by checking if the result is smaller than either of the input operands.

The overflow detection works because Size is assumed to be an unsigned integer type. In unsigned arithmetic, when an overflow occurs, the result wraps around to a smaller value. By checking if the sum is less than either addend, the function can reliably detect when an overflow has occurred.

When overflow is detected, the function raises an ERROR with ERRCODE_PROGRAM_LIMIT_EXCEEDED, indicating that the requested shared memory size exceeds what can be represented in a size_t variable. This prevents potentially dangerous memory allocation scenarios.

## Parameters / Member Variables
- : First Size value to add
- : Second Size value to add

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting function)
  - [errcode](../e/errcode.md)/errmsg (error reporting macros)
  - ERRCODE_PROGRAM_LIMIT_EXCEEDED (error code constant)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (extensively used for summing memory requirements)
  - BufferShmemSize (buffer pool size calculations)
  - [LockShmemSize](../L/LockShmemSize.md) (lock table size calculations)
  - [hash_estimate_size](../h/hash_estimate_size.md) (hash table size estimations)

## Notes and Other Information
- Critical for preventing integer overflow in shared memory size calculations
- The function assumes Size is an unsigned type (typically size_t)
- Overflow detection relies on the mathematical property that s1 + s2 < s1 when overflow occurs in unsigned arithmetic
- Widely used throughout PostgreSQL for accumulating shared memory requirements from different subsystems
- Essential for security as integer overflow in memory calculations could lead to buffer overflows
- The error thrown is fatal and will terminate the current operation/process initialization