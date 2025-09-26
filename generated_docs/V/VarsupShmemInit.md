# VarsupShmemInit

## Location
src/backend/access/transam/varsup.c: 47 - 76

## Overview
VarsupShmemInit initializes shared memory structures for transaction variables that are shared across all PostgreSQL backend processes.

## Definition
```c
void VarsupShmemInit(void)
```

## Detailed Description
VarsupShmemInit is responsible for initializing the shared memory segment that contains transaction-related global variables. The function uses `ShmemInitStruct` to either create or attach to the "TransamVariables" shared memory structure. When called by the postmaster process (during initial startup), it creates the structure and zeros it out. When called by child backend processes, it attaches to the existing shared memory structure. This function is critical for ensuring all PostgreSQL processes share the same view of transaction state.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitStruct
  - TransamVariablesData
  - FullTransactionId
- Called from (representative examples):
  - CreateOrAttachShmemStructs

## Notes and Other Information
- This function is part of the transaction variable support (varsup) subsystem
- Located in src/backend/access/transam/varsup.c:47-62
- Uses different behavior based on `IsUnderPostmaster` flag
- Postmaster process creates and initializes the shared memory structure
- Backend processes attach to existing shared memory structure
- Essential for maintaining consistent transaction state across all processes
- The function includes assertions to verify correct initialization behavior