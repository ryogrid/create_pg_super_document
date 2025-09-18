# MultiXactShmemSize

## Location
src/backend/access/transam/multixact.c: 1939 - 1943

## Overview
Calculates the total shared memory size required for PostgreSQL's multi-transaction (MultiXact) subsystem, including SLRU buffers and state structures.

## Definition
Size MultiXactShmemSize(void)

## Detailed Description
This function computes the shared memory requirements for the MultiXact subsystem, which handles multiple transaction IDs that can concurrently hold locks on the same tuple. The calculation includes:

1. **MultiXactState structure**: Contains per-backend MultiXactId arrays (two arrays per backend slot)
2. **Two SLRU areas**: 
   - MultiXact offset buffers (multixact_offset_buffers)
   - MultiXact member buffers (multixact_member_buffers)

The function uses PostgreSQL's size calculation macros to safely compute memory requirements while avoiding integer overflow. The MultiXact subsystem is essential for supporting tuple-level locking with multiple concurrent transactions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruShmemSize (called twice for offset and member buffers)
  - add_size (PostgreSQL size calculation macro)
  - mul_size (PostgreSQL size calculation macro)
  - offsetof (standard C macro)
- Global variables referenced:
  - multixact_offset_buffers (GUC parameter)
  - multixact_member_buffers (GUC parameter)  
  - MaxOldestSlot (maximum number of backend slots)
- Data types used:
  - Size (PostgreSQL size type)
  - MultiXactId (multi-transaction identifier type)
  - MultiXactStateData (shared state structure)
- Called from:
  - CalculateShmemSize (in src/backend/storage/ipc/ipci.c:132)
  - SizeOfMultiXactTruncate (referenced in src/include/access/multixact.h:124)

## Notes and Other Information
- The function allocates space for 2*MaxOldestSlot MultiXactId entries per backend to support the per-backend transaction ID tracking
- Uses the SHARED_MULTIXACT_STATE_SIZE macro to calculate the base state structure size
- Part of the MultiXact subsystem initialization process, called during shared memory setup
- The memory calculated here is allocated during PostgreSQL startup in MultiXactShmemInit()
- Critical for proper functioning of tuple-level locking in concurrent transaction scenarios