# mul_size

## Location
[src/backend/storage/ipc/shmem.c:510-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L510-L526)

## Overview
A utility function that safely multiplies two Size values while checking for arithmetic overflow, preventing shared memory size calculations from exceeding the limits of the size_t type.

## Definition

```c
Size
mul_size(Size s1, Size s2)
```
## Detailed Description
The  function performs safe multiplication of two Size values (typically size_t) with overflow detection. It is specifically designed for shared memory size calculations in PostgreSQL, where exceeding the maximum addressable memory size would cause undefined behavior. The function implements a division-based overflow check: after multiplication, it verifies that . If this condition fails, it indicates that overflow occurred during multiplication, and the function reports an ERROR with the specific error code . This safety mechanism is crucial in PostgreSQL's shared memory management subsystem, where accurate size calculations are essential for proper memory allocation and system stability.

## Parameters / Member Variables
- : First Size value to multiply (assumed to be unsigned)
- : Second Size value to multiply (assumed to be unsigned)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting when overflow detected)
  - ERROR (error level constant)
  - [errcode](../e/errcode.md) (for setting ERRCODE_PROGRAM_LIMIT_EXCEEDED)
  - [errmsg](../e/errmsg.md) (for error message formatting)

- Called from (representative examples):
  - [BTreeShmemSize](../B/BTreeShmemSize.md) (B-tree shared memory size calculation)
  - [XLOGShmemSize](../X/XLOGShmemSize.md) (transaction log shared memory sizing)
  - BufferShmemSize (buffer pool shared memory sizing)
  - [hash_estimate_size](../h/hash_estimate_size.md) (hash table size estimation)
  - [tuplesort_estimate_shared](../t/tuplesort_estimate_shared.md) (tuple sorting shared memory estimation)
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (parallel execution plan initialization)
  - [CreateSharedProcArray](../C/CreateSharedProcArray.md) (shared process array creation)
  - [LWLockShmemSize](../L/LWLockShmemSize.md) (lightweight lock shared memory sizing)

## Notes and Other Information
- The function assumes that Size is an unsigned type, which is critical for the overflow detection logic to work correctly
- Returns 0 immediately if either input parameter is 0, optimizing for common edge cases
- Extensively used throughout PostgreSQL's shared memory subsystem for safe size calculations
- The overflow check using division is mathematically sound for unsigned integer types
- Essential for preventing memory allocation failures and system crashes due to integer overflow in size calculations
- Located in src/backend/storage/ipc/shmem.c:510-526