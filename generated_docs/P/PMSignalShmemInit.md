# PMSignalShmemInit

## Location
[src/backend/storage/ipc/pmsignal.c:144-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L144-L180)

## Overview
Initializes the postmaster signaling system's shared memory structures during PostgreSQL startup, setting up the infrastructure for inter-process communication between postmaster and backend processes.

## Definition

```c
void
PMSignalShmemInit(void)
```
## Detailed Description
PMSignalShmemInit is responsible for initializing the shared memory segment used by the postmaster signaling system. It creates or attaches to the "PMSignalState" shared memory structure using ShmemInitStruct. If this is the first time the structure is created (not found in shared memory), the function performs complete initialization:

1. Zeros out all signal flags in the shared memory structure
2. Sets the number of child flags based on MaxLivePostmasterChildren()
3. Allocates the postmaster's private PMChildInUse array for tracking which child slots are in use
4. Initializes the next_child_inuse counter

The function handles both fresh initialization and reattachment scenarios, and includes logic to free and reallocate the PMChildInUse array if the maximum number of children has changed between restarts.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - PMSignalData (shared memory structure type)
  - ShmemInitStruct (shared memory initialization function)
  - PMSignalShmemSize (gets required memory size)
  - MemSet (memory initialization function)
  - unvolatize (macro for type casting)
  - MaxLivePostmasterChildren (gets max child process count)
  - MemoryContextAllocZero (memory allocation function)
  - pfree (memory deallocation function)
- Called from (representative examples):
  - CreateOrAttachShmemStructs (during shared memory setup)

## Notes and Other Information
- This is a public initialization function called during PostgreSQL startup
- Handles both fresh creation and reattachment to existing shared memory
- Includes logic to handle changing MaxLivePostmasterChildren configuration
- The PMChildInUse array is allocated in PostmasterContext for proper memory management
- Only allocates PMChildInUse array when running as postmaster (not standalone backend)
- Part of PostgreSQL's inter-process communication subsystem initialization