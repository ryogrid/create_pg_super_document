# ShmemAddrIsValid

## Location
[src/backend/storage/ipc/shmem.c:274-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L274-L282)

## Overview
ShmemAddrIsValid tests whether a given address refers to a location within the shared memory segment, returning true if the pointer points within the valid shared memory range.

## Definition

```c
bool
ShmemAddrIsValid(const void *addr)
```
## Detailed Description
This function provides a simple boundary check to determine if a given memory address falls within the allocated shared memory segment. It performs a range check by comparing the provided address against the shared memory segment's start (ShmemBase) and end (ShmemEnd) boundaries. This is useful for validation and debugging purposes to ensure that pointers being used actually reference shared memory locations.

The function is straightforward: it returns true if the address is greater than or equal to ShmemBase and less than ShmemEnd, effectively checking if the address lies within the [ShmemBase, ShmemEnd) range.

## Parameters / Member Variables
- : A constant pointer to the memory address to be validated

## Dependencies
- Functions called/Symbols referenced:
  - ShmemBase (global variable - start of shared memory segment)
  - ShmemEnd (global variable - end of shared memory segment)
- Called from (representative examples):
  - [ShmemInitStruct](ShmemInitStruct.md)
  - [ReleasePredXact](../R/ReleasePredXact.md)

## Notes and Other Information
- This is a simple validation utility function that performs no complex operations
- The function assumes ShmemBase and ShmemEnd have been properly initialized
- Returns false for NULL pointers or any address outside the shared memory bounds
- Commonly used for debugging and assertion checks in shared memory management code
- The address range check uses inclusive lower bound and exclusive upper bound [ShmemBase, ShmemEnd)

## Simplified Source

```c
// Simplified version of ShmemAddrIsValid
bool ShmemAddrIsValid(const void *addr) {
    // Check if address falls within shared memory bounds
    return (addr >= ShmemBase) && (addr < ShmemEnd);
}
```

Key simplifications made:
- No simplifications needed - function is already extremely simple and clear
- Original code contains only essential boundary checking logic
- Function performs a straightforward range validation with no complex operations