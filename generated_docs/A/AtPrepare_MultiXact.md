# AtPrepare_MultiXact

## Location
src/backend/access/transam/multixact.c: 1828 - 1841

## Overview
Saves MultiXact state during the prepare phase of a two-phase commit (2PC) transaction.

## Definition
```c
void AtPrepare_MultiXact(void)
```

## Detailed Description
This function is called during the prepare phase of two-phase commit transactions to preserve MultiXact-related state that needs to survive across the prepare/commit phases. It specifically saves the current process's OldestMemberMXactId value to the two-phase state file, but only if it represents a valid MultiXact ID. This preserved state is essential for maintaining proper MultiXact visibility and cleanup semantics across transaction boundaries in distributed transactions.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md)
  - TWOPHASE_RM_MULTIXACT_ID (resource manager constant)
- Global variables accessed:
  - OldestMemberMXactId[MyProcNumber]
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- Only called during two-phase commit prepare operations
- Conditionally saves state - only if OldestMemberMXactId is valid
- Uses TWOPHASE_RM_MULTIXACT_ID as the resource manager identifier
- The saved state will be recovered during the commit phase by multixact_twophase_recover
- Critical for maintaining MultiXact consistency in distributed transactions
- Part of PostgreSQL's two-phase commit protocol
- Located in src/backend/access/transam/multixact.c:1828-1841