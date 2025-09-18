# MultiXactAdvanceOldest

## Location
src/backend/access/transam/multixact.c: 2528 - 2544

## Overview
Updates the oldest MultiXactId value during WAL replay, but only if the new value is more recent than the current one.

## Definition
void MultiXactAdvanceOldest(MultiXactId oldestMulti, Oid oldestMultiDB)

## Detailed Description
This function is exclusively used during WAL replay to update the system's oldest MultiXactId value. It ensures that the oldest MultiXactId can only advance forward (to a more recent value), never backward, which maintains consistency during recovery operations. The function includes an assertion to ensure it's only called during recovery mode.

When the provided oldestMulti is indeed more recent than the current oldestMultiXactId, the function calls SetMultiXactIdLimit to update the limit and perform any necessary wraparound handling or cleanup operations.

## Parameters / Member Variables
- `oldestMulti`: The new oldest MultiXactId value to potentially set
- `oldestMultiDB`: The database OID associated with the oldest MultiXact

## Dependencies
- Functions called/Symbols referenced:
  - Assert (InRecovery)
  - MultiXactIdPrecedes
  - SetMultiXactIdLimit
- Called from (representative examples):
  - xlog_redo

## Notes and Other Information
- This function may ONLY be called during WAL replay (enforced by Assert)
- Only advances the oldest MultiXactId forward, never backward
- Uses SetMultiXactIdLimit to handle the actual update and any associated cleanup
- Critical for maintaining MultiXact consistency and preventing wraparound issues during recovery