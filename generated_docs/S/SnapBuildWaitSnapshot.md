# SnapBuildWaitSnapshot

## Location
[src/backend/replication/logical/snapbuild.c:1573-1619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L1573-L1619)

## Overview
Waits for all transactions older than a specified cutoff to finish and optionally logs a new xl_running_xacts record to assist with isolation testing and timely WAL record generation.

## Definition

```c
static void
SnapBuildWaitSnapshot(xl_running_xacts *running, TransactionId cutoff)
```
## Detailed Description
SnapBuildWaitSnapshot is a utility function that implements controlled waiting for specific transactions to complete during snapshot building. It serves both correctness and operational purposes in the logical replication system:

**Primary Function - Transaction Waiting:**
The function iterates through all transaction IDs in the running xacts record and waits for any transaction that is older than or equal to the specified cutoff to finish. This waiting is implemented using XactLockTableWait, which blocks until the target transaction commits or aborts.

**Safety Check:**
The function includes a critical safety check to prevent deadlocks by ensuring the current transaction never waits for itself. If such a condition is detected, an ERROR is raised.

**WAL Record Generation:**
After all required transactions have finished, the function attempts to generate a new xl_running_xacts record by calling LogStandbySnapshot(). This serves two purposes:
- Helps isolation testing tools detect when the snapshot building process is waiting
- Ensures timely generation of running xacts records without waiting for background processes

**Recovery Mode Handling:**
During recovery, the function cannot force generation of new WAL records, so it simply completes after waiting for the required transactions.

## Parameters / Member Variables
- : Pointer to xl_running_xacts record containing the list of currently running transactions
- : Transaction ID threshold - all transactions with IDs less than or equal to this value must finish

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)  
  - [XactLockTableWait](../X/XactLockTableWait.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [LogStandbySnapshot](../L/LogStandbySnapshot.md)
  - XLTW_None (wait mode constant)
- Called from (representative examples):
  - [SnapBuildFindSnapshot](SnapBuildFindSnapshot.md) (multiple times during state transitions)

## Notes and Other Information
- Static function used internally within the snapshot building subsystem
- The waiting mechanism is essential for ensuring snapshot consistency during logical replication initialization
- Uses transaction locking rather than polling for efficiency
- The ERROR condition for waiting on self should never occur in normal operation - indicates a logic error
- [LogStandbySnapshot](../L/LogStandbySnapshot.md)() call helps reduce delays in snapshot building progress
- Critical for the incremental snapshot building algorithm used in logical replication
- The function ensures that snapshot building doesn't proceed until it's safe to do so
- Part of the broader mechanism that ensures logical replication starts from a truly consistent point

## Simplified Source

```c
static void
SnapBuildWaitSnapshot(xl_running_xacts *running, TransactionId cutoff)
{
    // Wait for all transactions older than cutoff to finish
    for (int i = 0; i < running->xcnt; i++)
    {
        TransactionId xid = running->xids[i];

        // Safety check: never wait for ourselves
        if (TransactionIdIsCurrentTransactionId(xid))
            elog(ERROR, "waiting for ourselves");

        // Skip transactions newer than cutoff
        if (TransactionIdFollows(xid, cutoff))
            continue;

        // Wait for this transaction to finish
        XactLockTableWait(xid, NULL, NULL, XLTW_None);
    }

    // Generate new xl_running_xacts record if not in recovery
    if (!RecoveryInProgress())
    {
        LogStandbySnapshot();
    }
}
```