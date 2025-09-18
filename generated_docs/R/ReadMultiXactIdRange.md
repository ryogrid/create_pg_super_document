# ReadMultiXactIdRange

## Location
src/backend/access/transam/multixact.c: 790 - 813

## Overview
Gets the range of MultiXact IDs that may still be referenced by a relation, providing the oldest and next available MultiXact ID values.

## Definition


## Detailed Description
ReadMultiXactIdRange is a function that safely retrieves the current range of MultiXact IDs from the global MultiXact state. It acquires a shared lock on MultiXactGenLock to ensure consistent reading of the oldest MultiXact ID and the next available MultiXact ID. The function also performs bounds checking to ensure that returned values are never less than FirstMultiXactId, which represents the minimum valid MultiXact ID value.

This function is essential for operations that need to understand the valid range of MultiXact IDs, such as during vacuum operations or when determining which MultiXact data can be safely truncated.

## Parameters / Member Variables
- : Pointer to MultiXactId variable that will receive the oldest MultiXact ID that may still be referenced
- : Pointer to MultiXactId variable that will receive the next MultiXact ID to be assigned

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (with MultiXactGenLock, LW_SHARED)
  - LWLockRelease (with MultiXactGenLock)
  - MultiXactState (global state structure)
  - FirstMultiXactId (minimum valid MultiXact ID constant)
- Called from (representative examples):
  - Functions that need MultiXact ID range information for truncation decisions

## Notes and Other Information
- The function uses shared locking to allow concurrent reads while preventing inconsistent state during updates
- Bounds checking ensures that returned values are always within the valid MultiXact ID range
- The oldest ID represents the boundary below which MultiXact data may be safely truncated
- The next ID represents the next MultiXact ID that will be assigned to a new MultiXact