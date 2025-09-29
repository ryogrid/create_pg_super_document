# AssignTransactionId

## Location
[src/backend/access/transam/xact.c:632-787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L632-L787)

## Overview
Assigns a new permanent transaction ID to a given transaction state, ensuring parent transactions have XIDs assigned first and handling WAL logging requirements for subtransaction management.

## Definition
```c
static void AssignTransactionId(TransactionState s)
```

## Detailed Description
This is the core function responsible for assigning transaction IDs in PostgreSQL. It implements a comprehensive transaction ID assignment process that ensures proper ordering, WAL logging, and subtransaction hierarchy management. The function handles both top-level transactions and subtransactions with different logic paths.

Key responsibilities include:
1. **Parent XID Assignment**: Ensures parent transactions have XIDs before child transactions, maintaining the invariant that child XIDs are always greater than parent XIDs
2. **Parallel Operation Safety**: Prevents XID assignment during parallel operations where transaction synchronization could be problematic
3. **WAL Logging**: Manages WAL logging requirements for logical replication and standby servers
4. **Resource Management**: Handles transaction locks and resource ownership
5. **Subtransaction Tracking**: Maintains subtransaction hierarchy in pg_subtrans and shared memory

The function uses an iterative approach rather than deep recursion to assign XIDs to parent transactions, preventing stack overflow in deeply nested subtransaction hierarchies.

## Parameters / Member Variables
- `s`: TransactionState pointer to the transaction that needs an XID assignment

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid
  - [IsInParallelMode](../I/IsInParallelMode.md), IsParallelWorker
  - [GetNewTransactionId](../G/GetNewTransactionId.md)
  - [SubTransSetParent](../S/SubTransSetParent.md)
  - XidFromFullTransactionId
  - [RegisterPredicateLockingXid](../R/RegisterPredicateLockingXid.md)
  - [XactLockTableInsert](../X/XactLockTableInsert.md)
  - XLogLogicalInfoActive, XLogStandbyInfoActive
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
- Called from (representative examples):
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [GetTopFullTransactionId](../G/GetTopFullTransactionId.md)
  - [GetCurrentFullTransactionId](../G/GetCurrentFullTransactionId.md)
  - [AssignTransactionId](AssignTransactionId.md) (recursive)

## Notes and Other Information
- Static function - only accessible within the same source file
- Prevents stack overflow by using iterative parent XID assignment
- Enforces transaction ordering invariant (child XID > parent XID)
- Handles WAL logging for logical replication and hot standby requirements
- Critical for maintaining transaction visibility and MVCC consistency
- Located in src/backend/access/transam/xact.c:632-787
- Manages unreported XIDs for hot standby servers via XLOG_XACT_ASSIGNMENT records

## Simplified Source

```c
static void
AssignTransactionId(TransactionState s)
{
    bool isSubXact = (s->parent != NULL);
    bool log_unknown_top = false;

    // Verify transaction is ready for XID assignment
    Assert(!FullTransactionIdIsValid(s->fullTransactionId));
    Assert(s->state == TRANS_INPROGRESS);

    // Prevent XID assignment during parallel operations
    if (IsInParallelMode() || IsParallelWorker())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                       errmsg("cannot assign transaction IDs during a parallel operation")));

    // Ensure parent transactions have XIDs first (iterative approach)
    if (isSubXact && !FullTransactionIdIsValid(s->parent->fullTransactionId))
    {
        // Collect all parents without XIDs and assign them bottom-up
        TransactionState *parents = palloc(sizeof(TransactionState) * s->nestingLevel);
        size_t parentOffset = 0;
        TransactionState p = s->parent;

        while (p != NULL && !FullTransactionIdIsValid(p->fullTransactionId))
        {
            parents[parentOffset++] = p;
            p = p->parent;
        }

        while (parentOffset != 0)
            AssignTransactionId(parents[--parentOffset]);
        pfree(parents);
    }

    // Check if we need to log top-level XID for logical replication
    if (isSubXact && XLogLogicalInfoActive() && !TopTransactionStateData.didLogXid)
        log_unknown_top = true;

    // Generate new transaction ID and set up subtransaction hierarchy
    s->fullTransactionId = GetNewTransactionId(isSubXact);
    if (!isSubXact)
        XactTopFullTransactionId = s->fullTransactionId;

    if (isSubXact)
        SubTransSetParent(XidFromFullTransactionId(s->fullTransactionId),
                         XidFromFullTransactionId(s->parent->fullTransactionId));

    // Set up predicate locking and transaction lock for top-level transactions
    if (!isSubXact)
        RegisterPredicateLockingXid(XidFromFullTransactionId(s->fullTransactionId));

    XactLockTableInsert(XidFromFullTransactionId(s->fullTransactionId));

    // Handle WAL logging for subtransaction visibility on standby servers
    if (isSubXact && XLogStandbyInfoActive())
    {
        unreportedXids[nUnreportedXids] = XidFromFullTransactionId(s->fullTransactionId);
        nUnreportedXids++;

        // Log assignment record when we have enough unreported XIDs or need top-level logging
        if (nUnreportedXids >= PGPROC_MAX_CACHED_SUBXIDS || log_unknown_top)
        {
            xl_xact_assignment xlrec;
            xlrec.xtop = GetTopTransactionId();
            xlrec.nsubxacts = nUnreportedXids;

            XLogBeginInsert();
            XLogRegisterData((char *) &xlrec, MinSizeOfXactAssignment);
            XLogRegisterData((char *) unreportedXids, nUnreportedXids * sizeof(TransactionId));
            XLogInsert(RM_XACT_ID, XLOG_XACT_ASSIGNMENT);

            nUnreportedXids = 0;
            TopTransactionStateData.didLogXid = true;
        }
    }
}
```