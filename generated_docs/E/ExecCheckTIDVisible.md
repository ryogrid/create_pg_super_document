# ExecCheckTIDVisible

## Location
[src/backend/executor/nodeModifyTable.c:343-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L343-L372)

## Overview
A convenience variant of ExecCheckTupleVisible that fetches a tuple by its TID (tuple identifier) and then performs visibility checking for ON CONFLICT processing.

## Definition
```c
static void ExecCheckTIDVisible(EState *estate, ResultRelInfo *relinfo, ItemPointer tid, TupleTableSlot *tempSlot)
```

## Detailed Description
ExecCheckTIDVisible provides a streamlined interface for checking tuple visibility when only the tuple's TID is available. This function is specifically designed for ON CONFLICT scenarios where the system needs to verify that a conflicting tuple is visible according to MVCC snapshot rules.

The function workflow:
1. Performs a redundant isolation level check for efficiency
2. Fetches the tuple using the provided TID with SnapshotAny (to get any version)
3. Delegates the actual visibility checking to ExecCheckTupleVisible
4. Cleans up the temporary slot after processing

This function encapsulates the common pattern of "fetch tuple by TID, then check visibility" which is frequently needed during conflict resolution in INSERT operations with ON CONFLICT clauses.

## Parameters / Member Variables
- `estate`: Executor state containing snapshot and transaction information
- `relinfo`: Result relation information structure
- `tid`: ItemPointer (TID) identifying the tuple to check
- `tempSlot`: Temporary tuple slot used for fetching the tuple

## Dependencies
- Functions called/Symbols referenced:
  - IsolationUsesXactSnapshot
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - SnapshotAny
  - [ExecCheckTupleVisible](ExecCheckTupleVisible.md)
  - [ExecClearTuple](ExecClearTuple.md)
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md)

## Notes and Other Information
- This function is static to nodeModifyTable.c and used internally for ON CONFLICT processing
- The function uses SnapshotAny to fetch the tuple, which retrieves any version regardless of visibility, then relies on ExecCheckTupleVisible for proper MVCC visibility checking
- The temporary slot is cleaned up after use to ensure no resource leaks
- Error handling includes a specific message for ON CONFLICT scenarios when tuple fetching fails
- This function demonstrates PostgreSQL's layered approach to visibility checking, building higher-level convenience functions on top of core primitives

## Simplified Source

```c
static void
ExecCheckTIDVisible(EState *estate,
                    ResultRelInfo *relinfo,
                    ItemPointer tid,
                    TupleTableSlot *tempSlot)
{
    Relation rel = relinfo->ri_RelationDesc;

    // Skip check if not using transaction snapshots
    if (!IsolationUsesXactSnapshot())
        return;

    // Fetch the tuple by TID using any snapshot
    if (!table_tuple_fetch_row_version(rel, tid, SnapshotAny, tempSlot))
        elog(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");

    // Check if tuple is visible according to MVCC rules
    ExecCheckTupleVisible(estate, rel, tempSlot);

    // Clean up temporary slot
    ExecClearTuple(tempSlot);
}
```