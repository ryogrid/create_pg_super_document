# TwoPhaseShmemInit

## Location
[src/backend/access/transam/twophase.c:253-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L253-L293)

## Overview
Initializes the shared memory structures used by PostgreSQL's two-phase commit subsystem during database startup.

## Definition
void TwoPhaseShmemInit(void)

## Detailed Description
This function sets up the shared memory infrastructure for managing prepared transactions in PostgreSQL's two-phase commit protocol. It creates or attaches to the "Prepared Transaction Table" shared memory segment and initializes the data structures needed to track prepared transactions.

When running as the postmaster (not a child process), it performs full initialization including setting up a free list of GlobalTransactionData structures and associating each with a PGPROC entry from the PreparedXactProcs array. Child processes simply attach to the existing shared memory segment.

The function initializes the TwoPhaseState global variable, which serves as the entry point to all two-phase commit shared memory structures, and sets up the linked list management for efficient allocation and deallocation of transaction slots.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory initialization)
  - [TwoPhaseShmemSize](TwoPhaseShmemSize.md) (calculates required memory size)
  - GetNumberFromPGProc (maps PGPROC to process number)
  - MAXALIGN (memory alignment macro)
  - offsetof (standard C macro)
- Types referenced:
  - [GlobalTransaction](../G/GlobalTransaction.md) (pointer to transaction data)
  - [TwoPhaseStateData](TwoPhaseStateData.md) (main state structure)
- Global variables accessed:
  - TwoPhaseState (global state variable)
  - PreparedXactProcs (array of PGPROC entries)
  - max_prepared_xacts (GUC parameter)
- Called from:
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (in ipci.c:327)

## Notes and Other Information
- Only performs full initialization when IsUnderPostmaster is false (i.e., in the main postmaster process)
- Child processes simply attach to existing shared memory and skip initialization
- Creates a linked list of free GlobalTransactionData structures for efficient allocation
- Associates each transaction slot with a dedicated PGPROC entry for process management
- Part of PostgreSQL's shared memory initialization sequence during startup
- The 'found' parameter from ShmemInitStruct indicates whether the structure already existed
- Uses assertions to verify correct initialization state in debug builds