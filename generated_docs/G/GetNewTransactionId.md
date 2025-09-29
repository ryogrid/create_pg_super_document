# GetNewTransactionId

## Location
[src/backend/access/transam/varsup.c:77-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/varsup.c#L77-L287)

## Overview
GetNewTransactionId allocates the next FullTransactionId for a new transaction or subtransaction while implementing critical safety checks to prevent XID wraparound.

## Definition
```c
FullTransactionId GetNewTransactionId(bool isSubXact)
```

## Detailed Description
GetNewTransactionId is the core function responsible for allocating new transaction identifiers in PostgreSQL. It manages the global transaction counter in shared memory and implements critical safety mechanisms to prevent catastrophic XID wraparound. The function handles both regular transactions and subtransactions, enforcing limits through vacuum triggers, warnings, and ultimately preventing new transaction assignment when approaching wraparound thresholds. It also manages the shared ProcArray state and extends various transaction-related logs (CLOG, SUBTRANS, CommitTs) as needed.

The function implements a multi-layered protection system:
- Issues autovacuum requests when approaching xidVacLimit
- Issues warnings when approaching xidWarnLimit  
- Completely blocks new transactions when approaching xidStopLimit
- Special handling for bootstrap mode and recovery scenarios

## Parameters / Member Variables
- `isSubXact`: Boolean indicating whether this XID is for a subtransaction (true) or main transaction (false)

## Dependencies
- Functions called/Symbols referenced:
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - IsBootstrapProcessingMode
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XidFromFullTransactionId
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - [ExtendCLOG](../E/ExtendCLOG.md)
  - [ExtendCommitTs](../E/ExtendCommitTs.md)
  - [ExtendSUBTRANS](../E/ExtendSUBTRANS.md)
  - [FullTransactionIdAdvance](../F/FullTransactionIdAdvance.md)
  - [SendPostmasterSignal](../S/SendPostmasterSignal.md)
  - [get_database_name](../g/get_database_name.md)
- Called from (representative examples):
  - [AssignTransactionId](../A/AssignTransactionId.md)

## Notes and Other Information
- Located in src/backend/access/transam/varsup.c:77-287
- Acquires XidGenLock (LW_EXCLUSIVE) for safe concurrent access
- Implements XID wraparound protection through multiple threshold checks
- Updates both MyProc->xid and ProcGlobal->xids[] for visibility
- Handles subtransaction overflow by setting cache-overflowed flag
- Uses write barriers to prevent dangerous code reordering
- Cannot be called during parallel operations or recovery
- Returns special BootstrapTransactionId during bootstrap processing
- Critical for maintaining ACID properties and preventing data loss from XID wraparound

## Simplified Source

```c
FullTransactionId
GetNewTransactionId(bool isSubXact)
{
    FullTransactionId full_xid;
    TransactionId xid;

    // Prevent XID assignment during parallel operations
    if (IsInParallelMode())
        elog(ERROR, "cannot assign TransactionIds during a parallel operation");

    // Return special bootstrap XID during initialization
    if (IsBootstrapProcessingMode())
    {
        Assert(!isSubXact);
        MyProc->xid = BootstrapTransactionId;
        ProcGlobal->xids[MyProc->pgxactoff] = BootstrapTransactionId;
        return FullTransactionIdFromEpochAndXid(0, BootstrapTransactionId);
    }

    // Prevent XID assignment during recovery
    if (RecoveryInProgress())
        elog(ERROR, "cannot assign TransactionIds during recovery");

    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

    // Get next available XID
    full_xid = TransamVariables->nextXid;
    xid = XidFromFullTransactionId(full_xid);

    // Check wraparound safety limits
    if (TransactionIdFollowsOrEquals(xid, TransamVariables->xidVacLimit))
    {
        // Copy limits for safe access without lock
        TransactionId xidWarnLimit = TransamVariables->xidWarnLimit;
        TransactionId xidStopLimit = TransamVariables->xidStopLimit;
        TransactionId xidWrapLimit = TransamVariables->xidWrapLimit;
        Oid oldest_datoid = TransamVariables->oldestXidDB;

        LWLockRelease(XidGenLock);

        // Trigger autovacuum periodically
        if (IsUnderPostmaster && (xid % 65536) == 0)
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

        // Stop assignment if approaching wraparound limit
        if (IsUnderPostmaster && TransactionIdFollowsOrEquals(xid, xidStopLimit))
        {
            char *oldest_datname = get_database_name(oldest_datoid);
            ereport(ERROR,
                   (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("database is not accepting commands to avoid wraparound data loss"),
                    errhint("Execute a database-wide VACUUM.")));
        }
        // Issue warnings when approaching limits
        else if (TransactionIdFollowsOrEquals(xid, xidWarnLimit))
        {
            char *oldest_datname = get_database_name(oldest_datoid);
            ereport(WARNING,
                   (errmsg("database must be vacuumed within %u transactions",
                          xidWrapLimit - xid),
                    errhint("To avoid XID assignment failures, execute VACUUM.")));
        }

        // Re-acquire lock and reload values
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        full_xid = TransamVariables->nextXid;
        xid = XidFromFullTransactionId(full_xid);
    }

    // Extend transaction logs as needed
    ExtendCLOG(xid);
    ExtendCommitTs(xid);
    ExtendSUBTRANS(xid);

    // Advance to next XID
    FullTransactionIdAdvance(&TransamVariables->nextXid);

    // Update shared ProcArray state
    if (!isSubXact)
    {
        // Main transaction - store XID directly
        Assert(ProcGlobal->subxidStates[MyProc->pgxactoff].count == 0);
        MyProc->xid = xid;
        ProcGlobal->xids[MyProc->pgxactoff] = xid;
    }
    else
    {
        // Subtransaction - add to subtransaction list or mark overflow
        XidCacheStatus *substat = &ProcGlobal->subxidStates[MyProc->pgxactoff];
        int nxids = MyProc->subxidStatus.count;

        if (nxids < PGPROC_MAX_CACHED_SUBXIDS)
        {
            MyProc->subxids.xids[nxids] = xid;
            pg_write_barrier();
            MyProc->subxidStatus.count = substat->count = nxids + 1;
        }
        else
            MyProc->subxidStatus.overflowed = substat->overflowed = true;
    }

    LWLockRelease(XidGenLock);

    return full_xid;
}
```