# TwoPhaseGetDummyProcNumber

## Location
[src/backend/access/transam/twophase.c:903-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L903-L917)

## Overview
Retrieves the dummy proc number for a prepared transaction identified by its transaction ID, used for process identification in two-phase commit operations.

## Definition
ProcNumber TwoPhaseGetDummyProcNumber(TransactionId xid, bool lock_held)

## Detailed Description
This function returns the dummy proc number associated with a prepared transaction specified by its XID. Dummy proc numbers function similarly to proc numbers of real backends but are allocated starting from MaxBackends to ensure uniqueness across all currently active real backends and prepared transactions. This provides a consistent process identification mechanism for prepared transactions in the PostgreSQL two-phase commit protocol.

The function internally calls TwoPhaseGetGXact to locate the global transaction structure and then returns its pgprocno field.

## Parameters / Member Variables
- `xid`: TransactionId of the prepared transaction to look up
- `lock_held`: Boolean flag indicating whether the caller already holds TwoPhaseStateLock; if true, the function will not acquire the lock

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseGetGXact](TwoPhaseGetGXact.md)
  - GlobalTransaction
  - [PGPROC](../P/PGPROC.md)
- Called from (representative examples):
  - [PostPrepare_MultiXact](../P/PostPrepare_MultiXact.md)
  - [multixact_twophase_recover](../m/multixact_twophase_recover.md)
  - [multixact_twophase_postcommit](../m/multixact_twophase_postcommit.md)

## Notes and Other Information
- Dummy proc numbers start at MaxBackends to avoid conflicts with real backend process numbers
- The lock_held parameter provides flexibility for callers who already hold the necessary locks, improving performance in nested lock scenarios
- This function is primarily used in multixact operations during two-phase commit recovery and cleanup processes