# MaintainLatestCompletedXid

## Location
src/backend/storage/ipc/procarray.c: 967 - 988

## Overview
MaintainLatestCompletedXid updates the global latest completed transaction ID if the provided transaction ID is newer than the currently recorded one.

## Definition


## Detailed Description
This function is responsible for maintaining the global record of the latest completed transaction ID in TransamVariables->latestCompletedXid. It compares the provided transaction ID with the current latest completed XID and updates the global value if the provided XID is newer. This tracking is essential for PostgreSQL's MVCC (Multi-Version Concurrency Control) system to determine transaction visibility and manage snapshot generation.

The function includes several assertions to ensure it's called in the correct context: during normal processing (not recovery), with proper locking, and with valid transaction IDs. It uses PostgreSQL's transaction ID comparison logic that handles wraparound.

## Parameters / Member Variables
- : The transaction ID that potentially represents a newly completed transaction

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - LWLockHeldByMe
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - XidFromFullTransactionId
  - [FullXidRelativeTo](../F/FullXidRelativeTo.md)
  - IsBootstrapProcessingMode
  - FullTransactionIdIsNormal
- Called from (representative examples):
  - [ProcArrayRemove](../P/ProcArrayRemove.md)
  - [ProcArrayEndTransactionInternal](../P/ProcArrayEndTransactionInternal.md)
  - [XidCacheRemoveRunningXids](../X/XidCacheRemoveRunningXids.md)

## Notes and Other Information
- Must be called while holding ProcArrayLock to ensure atomic updates
- Only operates during normal processing, not during recovery
- Uses TransactionIdPrecedes to handle transaction ID wraparound correctly
- Maintains consistency of the global latestCompletedXid for snapshot generation
- Part of PostgreSQL's core MVCC infrastructure for transaction visibility
- The latestCompletedXid is used by snapshot generation to determine which transactions are definitely completed