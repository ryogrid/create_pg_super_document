# ProcArrayInitRecovery

## Location
src/backend/storage/ipc/procarray.c: 1023 - 1053

## Overview
ProcArrayInitRecovery initializes the recovery transaction ID management environment by setting up the tracking of the latest observed XID during standby recovery.

## Definition


## Detailed Description
This function is called during recovery startup to establish the baseline for transaction ID tracking in a standby server. It sets the latestObservedXid variable to indicate how far the CLOG (commit log) and SUBTRANS (subtransaction) storage have been initialized. This information is crucial for ensuring gapless initialization of these storage systems as recovery progresses.

The function retreats the provided XID by one using TransactionIdRetreat, which ensures that subsequent recovery operations can safely extend from this point onwards. This is used by RecordKnownAssignedTransactionIds and ProcArrayApplyRecoveryInfo to maintain consistency during recovery.

## Parameters / Member Variables
- : The transaction ID up to which CLOG and SUBTRANS have been initialized

## Dependencies
- Functions called/Symbols referenced:
  - STANDBY_INITIALIZED
  - TransactionIdIsNormal
  - TransactionIdRetreat
- Called from (representative examples):
  - StartupXLOG

## Notes and Other Information
- Only called during standby recovery initialization when standbyState is STANDBY_INITIALIZED
- Essential for maintaining gapless CLOG and SUBTRANS initialization during recovery
- The latestObservedXid is used as a baseline for extending transaction tracking during recovery
- Uses TransactionIdRetreat to ensure safe starting point for subsequent recovery operations
- Part of PostgreSQL's hot standby and streaming replication infrastructure
- Critical for maintaining consistency in transaction visibility during recovery