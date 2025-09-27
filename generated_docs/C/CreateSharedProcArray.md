# CreateSharedProcArray

## Location
[src/backend/storage/ipc/procarray.c:418-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L418-L467)

## Overview
Initializes the shared PGPROC array during postmaster startup, setting up shared memory structures for tracking active processes and their transaction information.

## Definition

```c
structure */
	procArray = (ProcArrayStruct *)
		ShmemInitStruct("Proc Array",
						add_size(offsetof(ProcArrayStruct, pgprocnos),
								 mul_size(sizeof(int),
										  PROCARRAY_MAXPROCS)),
						&found);
```
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

## Simplified Source

```c
// Simplified version of CreateSharedProcArray
void CreateSharedProcArray(void) {
    bool found;

    // Step 1: Create or attach to main ProcArray shared memory structure
    procArray = (ProcArrayStruct *)
        ShmemInitStruct("Proc Array",
                       calculated_procarray_size,
                       &found);

    // Step 2: Initialize structure if we're the first process
    if (!found) {
        // Initialize process tracking counters
        procArray->numProcs = 0;
        procArray->maxProcs = PROCARRAY_MAXPROCS;

        // Initialize transaction ID tracking for hot standby
        procArray->maxKnownAssignedXids = TOTAL_MAX_CACHED_SUBXIDS;
        procArray->numKnownAssignedXids = 0;
        procArray->tailKnownAssignedXids = 0;
        procArray->headKnownAssignedXids = 0;
        procArray->lastOverflowedXid = InvalidTransactionId;

        // Initialize replication slot tracking
        procArray->replication_slot_xmin = InvalidTransactionId;
        procArray->replication_slot_catalog_xmin = InvalidTransactionId;

        // Initialize transaction completion counter
        TransamVariables->xactCompletionCount = 1;
    }

    // Step 3: Set global reference to all processes
    allProcs = ProcGlobal->allProcs;

    // Step 4: Initialize hot standby arrays if needed
    if (EnableHotStandby) {
        // Create shared arrays for tracking known assigned transaction IDs
        KnownAssignedXids = (TransactionId *)
            ShmemInitStruct("KnownAssignedXids",
                           known_xids_array_size,
                           &found);

        KnownAssignedXidsValid = (bool *)
            ShmemInitStruct("KnownAssignedXidsValid",
                           validity_array_size,
                           &found);
    }
}
```

Key simplifications made:
- Replaced complex size calculations with descriptive variable names
- Added step-by-step comments explaining the initialization process
- Grouped related field initializations with explanatory comments
- Focused on the main execution path and key functionality
- Abstracted low-level size calculation details while preserving logic