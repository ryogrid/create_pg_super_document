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
  - [PMSignalData](PMSignalData.md) (shared memory structure type)
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory initialization function)
  - [PMSignalShmemSize](PMSignalShmemSize.md) (gets required memory size)
  - MemSet (memory initialization function)
  - unvolatize (macro for type casting)
  - [MaxLivePostmasterChildren](../M/MaxLivePostmasterChildren.md) (gets max child process count)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (memory allocation function)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (during shared memory setup)

## Notes and Other Information
- This is a public initialization function called during PostgreSQL startup
- Handles both fresh creation and reattachment to existing shared memory
- Includes logic to handle changing MaxLivePostmasterChildren configuration
- The PMChildInUse array is allocated in PostmasterContext for proper memory management
- Only allocates PMChildInUse array when running as postmaster (not standalone backend)
- Part of PostgreSQL's inter-process communication subsystem initialization

## Simplified Source

```c
// Simplified version of PMSignalShmemInit
void PMSignalShmemInit(void) {
    bool found;

    // Initialize or attach to shared memory structure for PM signals
    PMSignalState = (PMSignalData *)
        ShmemInitStruct("PMSignalState", PMSignalShmemSize(), &found);

    if (!found) {
        // First time initialization - clear all signal flags
        MemSet(PMSignalState, 0, PMSignalShmemSize());

        // Set up child process tracking
        num_child_inuse = MaxLivePostmasterChildren();
        PMSignalState->num_child_flags = num_child_inuse;

        // Allocate postmaster's private array for tracking child slots
        if (PostmasterContext != NULL) {
            if (PMChildInUse)
                pfree(PMChildInUse);  // Free old array if exists
            PMChildInUse = (bool *)
                MemoryContextAllocZero(PostmasterContext,
                                     num_child_inuse * sizeof(bool));
        }

        // Initialize child slot counter
        next_child_inuse = 0;
    }
}
```

Key simplifications made:
- Removed detailed comments about reallocation logic
- Simplified memory allocation explanation
- Consolidated variable declarations
- Focused on the two main paths: shared memory creation vs. attachment
- Preserved essential initialization steps and memory management