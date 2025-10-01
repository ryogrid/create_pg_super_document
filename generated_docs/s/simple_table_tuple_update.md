# simple_table_tuple_update

## Location
[src/backend/access/table/tableam.c:336-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L336-L382)

## Overview
A simplified interface for updating a tuple in a table when concurrent updates are not expected, providing error handling for all failure cases through ereport().

## Definition

```c
void
simple_table_tuple_update(Relation rel, ItemPointer otid,
						  TupleTableSlot *slot,
						  Snapshot snapshot,
						  TU_UpdateIndexes *update_indexes)
```
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
  - [table_tuple_update](../t/table_tuple_update.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - elog
  - TM_Result (enum values: TM_SelfModified, TM_Ok, TM_Updated, TM_Deleted)
  - [TM_FailureData](../T/TM_FailureData.md)
  - [LockTupleMode](../L/LockTupleMode.md)
  - InvalidSnapshot
- Called from (representative examples):
  - [ExecSimpleRelationUpdate](../E/ExecSimpleRelationUpdate.md)
  - [table_scan_sample_next_tuple](../t/table_scan_sample_next_tuple.md)

## Notes and Other Information
- This is a convenience function that simplifies error handling for tuple updates
- Should only be used when concurrent updates are not expected due to proper locking
- All failure cases result in ERROR-level messages, making this unsuitable for cases where graceful handling of update conflicts is needed
- The function automatically increments the command ID, indicating it expects to be called for write operations
- Located in the table access method layer, providing a high-level interface to the storage engine

## Simplified Source

```c
void simple_table_tuple_update(Relation rel, ItemPointer otid,
                               TupleTableSlot *slot, Snapshot snapshot,
                               TU_UpdateIndexes *update_indexes) {
    TM_Result result;
    TM_FailureData tmfd;
    LockTupleMode lockmode;

    // Attempt to update the tuple with default parameters
    result = table_tuple_update(rel, otid, slot, GetCurrentCommandId(true),
                               snapshot, InvalidSnapshot, true /* wait for commit */,
                               &tmfd, &lockmode, update_indexes);

    // Handle results - only TM_Ok is acceptable, all others are errors
    switch (result) {
        case TM_SelfModified:
            elog(ERROR, "tuple already updated by self");
            break;
        case TM_Ok:
            /* Success - done */
            break;
        case TM_Updated:
            elog(ERROR, "tuple concurrently updated");
            break;
        case TM_Deleted:
            elog(ERROR, "tuple concurrently deleted");
            break;
        default:
            elog(ERROR, "unrecognized table_tuple_update status: %u", result);
    }
}
```