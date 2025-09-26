# StandbyRecoverPreparedTransactions

## Location
[src/backend/access/transam/twophase.c:2033-2073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L2033-L2073)

## Overview
StandbyRecoverPreparedTransactions sets up prepared transactions in standby recovery mode to allow standby queries to correctly treat them as active transactions.

## Definition
```c
void StandbyRecoverPreparedTransactions(void)
```

## Detailed Description
StandbyRecoverPreparedTransactions is called during hot standby recovery to properly initialize prepared transactions for standby query processing. Unlike RecoverPreparedTransactions (used at the end of recovery), this function is specifically designed for standby servers that need to maintain prepared transaction visibility during ongoing recovery.

The function operates by:
1. Acquiring exclusive lock on TwoPhaseStateLock for atomic processing
2. Iterating through all prepared transactions in TwoPhaseState
3. Processing each transaction's buffer data with ProcessTwoPhaseBuffer
4. Using specific flags (ondisk, true, false) appropriate for standby recovery context

The key purpose is to ensure that pg_subtrans is properly updated so that any subtransactions belonging to prepared transactions will be correctly visible as in-progress in snapshots taken during recovery. This maintains transaction isolation and consistency for read queries executed on the standby server.

## Parameters / Member Variables
This function takes no parameters and operates on the global TwoPhaseState.

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [pfree](../p/pfree.md)
- Called from:
  - [StartupXLOG](StartupXLOG.md)
  - [xlog_redo](../x/xlog_redo.md)

## Notes and Other Information
- Never called at the end of recovery - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md) is used instead
- Specifically designed for hot standby scenarios during ongoing recovery
- Updates pg_subtrans to maintain proper subtransaction visibility
- Ensures prepared transactions appear active in snapshots during standby queries
- Uses exclusive locking to prevent concurrent modifications during processing
- Critical for maintaining ACID properties on standby servers during recovery