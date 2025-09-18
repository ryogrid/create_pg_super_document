# CreateSharedProcArray

## Location
src/backend/storage/ipc/procarray.c: 418 - 467

## Overview
Initializes the shared PGPROC array during postmaster startup, setting up shared memory structures for tracking active processes and their transaction information.

## Definition


## Detailed Description
CreateSharedProcArray is responsible for initializing the shared process array infrastructure during PostgreSQL postmaster startup. It creates or attaches to the shared memory structure that tracks all active backend processes in the system. The function sets up the main ProcArray structure with initial values and, if hot standby is enabled, also initializes the KnownAssignedXids tracking arrays for replication purposes.

The function uses shared memory initialization to ensure that all processes in the PostgreSQL cluster can access the same process tracking information. If this is the first process to initialize the structure, it sets all fields to their initial values. Otherwise, it simply attaches to the existing shared memory segment.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [add_size](../a/add_size.md)
  - [mul_size](../m/mul_size.md)
  - [ProcArrayStruct](../P/ProcArrayStruct.md)
  - PROCARRAY_MAXPROCS
  - TOTAL_MAX_CACHED_SUBXIDS
  - EnableHotStandby
  - ProcGlobal
  - TransamVariables

- Called from (representative examples):
  - [CreateOrAttachShmemStructs](CreateOrAttachShmemStructs.md)

## Notes and Other Information
- This function is called only once during postmaster startup
- The function handles both the case where it's the first to initialize the shared memory and when attaching to existing shared memory
- Hot standby functionality requires additional shared memory structures for tracking known assigned transaction IDs
- The procArray global variable is set to point to the shared memory structure
- Initial values include setting numProcs to 0, indicating no active processes at startup
- Transaction completion count is initialized to 1 to avoid wraparound issues