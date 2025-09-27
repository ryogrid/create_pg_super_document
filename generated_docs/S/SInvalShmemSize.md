# SInvalShmemSize

## Location
[src/backend/storage/ipc/sinvaladt.c:218-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L218-L233)

## Overview
SInvalShmemSize calculates and returns the amount of shared memory space required for the shared invalidation subsystem's data structures.

## Definition
Size SInvalShmemSize(void)

## Detailed Description
This function computes the total shared memory allocation needed for the shared invalidation ADT (Abstract Data Type) by calculating the size of the SISeg structure including its variable-length arrays. The calculation accounts for:

1. The base size of the SISeg structure up to the procState field
2. The procState array sized for NumProcStateSlots processes
3. The pgprocnos array sized for NumProcStateSlots processes

The function uses PostgreSQL's safe arithmetic functions (add_size and mul_size) to prevent integer overflow during size calculations.

## Parameters / Member Variables
(No parameters - function takes void)

## Dependencies
- Functions called/Symbols referenced:
  - offsetof (standard C macro)
  - [add_size](../a/add_size.md)
  - [mul_size](../m/mul_size.md)
- Data types referenced:
  - [SISeg](SISeg.md)
  - [ProcState](../P/ProcState.md)
  - Size
- [Variables](../V/Variables.md) referenced:
  - NumProcStateSlots
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md)
  - [CreateSharedInvalidationState](../C/CreateSharedInvalidationState.md)

## Notes and Other Information
- This function is part of the shared memory initialization process and must be called before allocating shared memory for the invalidation subsystem
- The size calculation is critical for proper shared memory layout and must account for all processes that will participate in cache invalidation
- Uses safe arithmetic functions to prevent overflow, which is important for large numbers of backend processes

## Simplified Source

```c
// Simplified version of SInvalShmemSize
Size SInvalShmemSize(void) {
    Size size;

    // Calculate base size of SISeg structure up to procState field
    size = offsetof(SISeg, procState);

    // Add space for procState array (one entry per process slot)
    size = add_size(size, mul_size(sizeof(ProcState), NumProcStateSlots));

    // Add space for pgprocnos array (one int per process slot)
    size = add_size(size, mul_size(sizeof(int), NumProcStateSlots));

    return size;
}
```

Key simplifications made:
- Added descriptive comments for each calculation step
- Preserved the original logic completely (no simplification needed as function is already concise)
- Maintained use of safe arithmetic functions for overflow protection
- Focused on the three-step calculation: base size + procState array + pgprocnos array