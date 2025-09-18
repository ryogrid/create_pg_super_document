# simple_table_tuple_update

## Location
src/backend/access/table/tableam.c: 336 - 382

## Overview
A simplified interface for updating a tuple in a table when concurrent updates are not expected, providing error handling for all failure cases through ereport().

## Definition


## Detailed Description
This function provides a wrapper around  that is designed for scenarios where concurrent tuple updates are not anticipated (e.g., when holding an appropriate lock on the relation). Unlike the lower-level , this function does not return status codes to the caller. Instead, it handles all possible failure scenarios internally and reports any errors via , making it suitable for contexts where update failures should be treated as fatal errors.

The function calls  with specific parameters:
- Uses the current command ID with increment (via )
- Uses  for the cross-check snapshot
- Waits for commit ()

All possible return values from  are handled:
- : Success case, function returns normally
- : Tuple was already updated in current command - throws ERROR
- : Tuple was concurrently updated - throws ERROR  
- : Tuple was concurrently deleted - throws ERROR

## Parameters / Member Variables
- : Relation containing the tuple to be updated
- : ItemPointer to the old tuple's location (TID)
- : TupleTableSlot containing the new tuple data
- : Snapshot to use for the update operation
- : Pointer to TU_UpdateIndexes structure controlling index update behavior

## Dependencies
- Functions called/Symbols referenced:
  - table_tuple_update
  - GetCurrentCommandId
  - elog
  - TM_Result (enum values: TM_SelfModified, TM_Ok, TM_Updated, TM_Deleted)
  - TM_FailureData
  - LockTupleMode
  - InvalidSnapshot
- Called from (representative examples):
  - ExecSimpleRelationUpdate
  - table_scan_sample_next_tuple

## Notes and Other Information
- This is a convenience function that simplifies error handling for tuple updates
- Should only be used when concurrent updates are not expected due to proper locking
- All failure cases result in ERROR-level messages, making this unsuitable for cases where graceful handling of update conflicts is needed
- The function automatically increments the command ID, indicating it expects to be called for write operations
- Located in the table access method layer, providing a high-level interface to the storage engine