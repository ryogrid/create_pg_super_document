# ShmemBackendArrayAdd

## Location
[src/backend/postmaster/postmaster.c:4566-4575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4566-L4575)

## Overview
Adds a backend process entry to the shared memory backend array at the designated slot position.

## Definition
```c
static void ShmemBackendArrayAdd(Backend *bn)
```

## Detailed Description
This function adds a backend process entry to the ShmemBackendArray at a specific position determined by the backend's child_slot value. The function calculates the array index by subtracting 1 from the child_slot (since slots are 1-indexed but arrays are 0-indexed). It includes an assertion to verify that the target slot is empty before adding the backend entry, ensuring data integrity in the shared memory structure.

## Parameters / Member Variables
- `bn`: Pointer to the Backend structure containing the process information to be added to the array

## Dependencies
- Functions called/Symbols referenced:
  - [Backend](../B/Backend.md) (data structure type)
  - Assert (debugging assertion macro)
- Called from:
  - [win32_deadchild_waitinfo](../w/win32_deadchild_waitinfo.md) (src/backend/postmaster/postmaster.c:465)
  - [BackendStartup](../B/BackendStartup.md) (src/backend/postmaster/postmaster.c:3627)
  - [StartAutovacuumWorker](StartAutovacuumWorker.md) (src/backend/postmaster/postmaster.c:4005)
  - [do_start_bgworker](../d/do_start_bgworker.md) (src/backend/postmaster/postmaster.c:4295)

## Notes and Other Information
- This is a static function, only accessible within the postmaster.c file
- The function assumes the child_slot value is valid and greater than 0
- Uses array indexing (child_slot - 1) to convert from 1-based slot numbering to 0-based array indexing
- Part of the postmaster's process tracking mechanism for managing backend processes
- Located in src/backend/postmaster/postmaster.c:4566-4575

## Simplified Source

```c
// Simplified version of ShmemBackendArrayAdd
static void ShmemBackendArrayAdd(Backend *bn) {
    // Convert 1-based child slot to 0-based array index
    int array_index = bn->child_slot - 1;

    // Verify the target slot is empty (debugging check)
    Assert(ShmemBackendArray[array_index].pid == 0);

    // Copy the backend structure to the shared memory array
    ShmemBackendArray[array_index] = *bn;
}
```

Key simplifications made:
- Added descriptive variable name `array_index` instead of `i`
- Added clear comments explaining each step
- Preserved the essential logic: slot conversion, assertion check, and structure copy
- Maintained the original function's simplicity as it was already quite streamlined