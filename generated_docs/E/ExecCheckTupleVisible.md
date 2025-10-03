# ExecCheckTupleVisible

## Location
[src/backend/executor/nodeModifyTable.c:309-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L309-L342)

## Overview
Verifies that a tuple is visible according to MVCC snapshot consistency rules and raises serialization failures when necessary to maintain isolation level guarantees.

## Definition
```c
static void ExecCheckTupleVisible(EState *estate, Relation rel, TupleTableSlot *slot)
```

## Detailed Description
ExecCheckTupleVisible ensures that operations maintain consistency with higher isolation levels by checking if a tuple is visible to the current transaction's MVCC snapshot. This function is crucial for preventing inconsistent behavior when dealing with speculative insertions or conflict resolution.

The function operates as follows:
1. First checks if the current isolation level uses transaction snapshots - if not, returns immediately
2. Uses the table access method to verify if the tuple satisfies the current snapshot
3. If the tuple is not visible, extracts the xmin (transaction ID that created the tuple)
4. Checks if the conflicting transaction is the current transaction - if so, allows the operation to proceed
5. If the conflict is with another transaction, raises a serialization failure error

This mechanism is essential for maintaining ACID properties, particularly in scenarios involving ON CONFLICT handling and speculative insertions.

## Parameters / Member Variables
- `estate`: Executor state containing snapshot and transaction information
- `rel`: The relation being accessed
- `slot`: Tuple slot containing the tuple to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - IsolationUsesXactSnapshot
  - [table_tuple_satisfies_snapshot](../t/table_tuple_satisfies_snapshot.md)
  - [slot_getsysattr](../s/slot_getsysattr.md)
  - MinTransactionIdAttributeNumber
  - [DatumGetTransactionId](../D/DatumGetTransactionId.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - ERRCODE_T_R_SERIALIZATION_FAILURE
- Called from (representative examples):
  - [ExecCheckTIDVisible](ExecCheckTIDVisible.md)
  - [ExecOnConflictUpdate](ExecOnConflictUpdate.md)

## Notes and Other Information
- This function is static to nodeModifyTable.c and used internally for MVCC consistency checks
- The function specifically handles the case where conflicting keys are proposed for insertion in a single command by allowing operations within the same transaction
- Serialization failures are only raised when conflicts occur with other transactions, not with the current transaction
- This is a key component in PostgreSQL's implementation of serializable isolation levels
- The function is particularly important for ON CONFLICT handling where tuple visibility affects conflict resolution decisions

## Simplified Source

```c
static void ExecCheckTupleVisible(EState *estate, Relation rel, TupleTableSlot *slot) {
    // Only check visibility for isolation levels that use transaction snapshots
    if (!IsolationUsesXactSnapshot()) {
        return;
    }

    // Check if tuple satisfies current MVCC snapshot
    if (!table_tuple_satisfies_snapshot(rel, slot, estate->es_snapshot)) {
        // Get transaction ID that created this tuple
        Datum xminDatum = slot_getsysattr(slot, MinTransactionIdAttributeNumber, &isnull);
        TransactionId xmin = DatumGetTransactionId(xminDatum);

        // Allow conflicts within same transaction (e.g., multiple inserts in one command)
        // Only raise serialization failure for conflicts with other transactions
        if (!TransactionIdIsCurrentTransactionId(xmin)) {
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                          errmsg("could not serialize access due to concurrent update")));
        }
    }
}
```