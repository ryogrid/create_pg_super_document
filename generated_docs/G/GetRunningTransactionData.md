# GetRunningTransactionData

## Location
[src/backend/storage/ipc/procarray.c:2693-2878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2693-L2878)

## Overview
GetRunningTransactionData returns comprehensive information about all currently running transactions, including both main transactions and subtransactions, primarily used for checkpointing and standby server coordination.

## Definition


## Detailed Description
This function collects and returns detailed information about all running transactions in the system. Unlike GetSnapshotData which is optimized for snapshot creation, GetRunningTransactionData provides more comprehensive information including VACUUM processes and prepared transactions. It is specifically designed for checkpointing operations and standby server coordination.

The function acquires both XidGenLock and ProcArrayLock to ensure consistency during data collection. The caller is responsible for releasing these locks after WAL-logging the snapshot information. This locking strategy prevents new XIDs from entering the proc array and transactions from committing until the snapshot is safely recorded.

The function allocates memory statically and returns a pointer to this static structure, making it non-reentrant. It collects both main transaction IDs and subtransaction IDs, handling cases where subtransaction caches have overflowed.

Key behaviors include:
- Collects all transactions with valid TransactionIDs
- Tracks oldest running transaction globally and per-database
- Handles subtransaction overflow scenarios
- Includes prepared transactions (dummy PGPROCs)
- Never executed during recovery (no KnownAssignedXids handling needed)

## Parameters / Member Variables
This function takes no parameters but returns a RunningTransactions structure containing:
- : Count of main transactions
- : Count of subtransactions  
- : Status indicating if subtransactions are in array or subtrans
- : Next transaction ID to be assigned
- : Oldest transaction ID still running system-wide
- : Oldest transaction ID running in current database
- : Most recent completed transaction ID
- : Array containing all collected transaction IDs

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)  
  - XidFromFullTransactionId
  - TransactionIdIsValid
  - TransactionIdIsNormal
  - LWLockAcquire
  - UINT32_ACCESS_ONCE
  - pg_read_barrier
  - malloc
- Called from (representative examples):
  - LogStandbySnapshot (src/backend/storage/ipc/standby.c:1306)

## Notes and Other Information
- Only executed during normal operation, never during recovery
- Caller must release XidGenLock and ProcArrayLock after use
- Returns statically allocated data structure - not thread safe
- Memory for transaction ID array is allocated once and reused
- Designed primarily for background writer process during checkpoints
- Handles subtransaction overflow by setting appropriate status flags
- Does not update snapshot counters, leaving that to GetSnapshotData
- Includes duplicate TransactionIds from prepared transactions finishing preparation