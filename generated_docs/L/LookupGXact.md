# LookupGXact

## Location
src/backend/access/transam/twophase.c: 2624 - 2674

## Overview
LookupGXact checks if a prepared transaction with the given Global Transaction ID (GID), LSN, and timestamp exists in the current PostgreSQL instance's prepared transaction list.

## Definition


## Detailed Description
This function is primarily used in logical replication scenarios to verify whether a prepared transaction received from an upstream (remote) node already exists locally. The function performs a comprehensive match check using three criteria: GID, origin LSN, and origin timestamp. This multi-criteria matching is essential because different prepared transactions with the same GID can exist on the same node, and matching only the GID would be insufficient to distinguish between transactions from different nodes.

The function acquires a shared lock on TwoPhaseStateLock and iterates through all currently prepared transactions. For each transaction with a matching GID, it reads the transaction's header data (either from disk using ReadTwoPhaseFile or from WAL using XlogReadTwoPhaseData) and compares the origin_lsn and origin_timestamp values to ensure an exact match.

The LSN comparison uses the prepare_end_lsn (where the prepare phase ends) because this is what gets stored as origin_lsn in the two-phase commit file. This design choice ensures consistency in LSN tracking across the two-phase commit process.

## Parameters / Member Variables
- : The Global Transaction ID (string identifier) of the prepared transaction to search for
- : The LSN position where the prepare phase ended, used for matching the origin_lsn stored in the transaction header
- : The timestamp when the transaction was originally prepared, used for additional verification to distinguish transactions from different nodes

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - ReadTwoPhaseFile
  - XlogReadTwoPhaseData
  - TwoPhaseFileHeader
  - GlobalTransaction
  - LW_SHARED
- Called from (representative examples):
  - apply_handle_rollback_prepared

## Notes and Other Information
- The function holds TwoPhaseStateLock in shared mode during the entire operation, including I/O operations, for simplicity
- There is a noted optimization opportunity to move I/O operations outside the lock if GID collisions become frequent between publisher and subscriber
- The function only considers valid GXACTs (those with gxact->valid set to true)
- For on-disk transactions, it uses ReadTwoPhaseFile; for in-memory transactions, it uses XlogReadTwoPhaseData with the prepare_start_lsn
- This function is crucial for maintaining consistency in distributed PostgreSQL environments with logical replication
- The multi-criteria matching (GID + LSN + timestamp) prevents false positives when the same GID appears on different nodes