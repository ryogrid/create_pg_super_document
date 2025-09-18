# AtPrepare_Locks

## Location
src/backend/storage/lmgr/lock.c: 3304 - 3399

## Overview
AtPrepare_Locks performs preparatory work for PREPARE TRANSACTION by creating 2PC state file records for all transaction-level locks currently held.

## Definition
```c
void AtPrepare_Locks(void)
```

## Detailed Description
This function is called during PREPARE TRANSACTION processing to serialize all transaction-level locks into the two-phase commit state file. It performs the following key operations:

1. **Validation**: Calls CheckForSessionAndXactLocks() to ensure there are no conflicts between session-level and transaction-level locks on the same object
2. **Lock enumeration**: Scans the local lock table (LockMethodLocalHash) to find all currently held locks
3. **Filtering**: Excludes session-level locks and virtual transaction (VXID) locks from serialization
4. **Fast-path handling**: Moves any fast-path locks to the main lock table to ensure they can be properly managed during recovery
5. **2PC record creation**: Creates TwoPhaseLockRecord entries for each qualifying lock and registers them via RegisterTwoPhaseRecord()

The function ensures that only transaction-level locks are preserved across the PREPARE/COMMIT PREPARED boundary, while maintaining proper reference counting for strong locks.

## Parameters / Member Variables
This function takes no parameters and operates on global lock state.

## Dependencies
- Functions called/Symbols referenced:
  - CheckForSessionAndXactLocks
  - hash_seq_init
  - hash_seq_search
  - FastPathGetRelationLockEntry
  - RegisterTwoPhaseRecord
  - TwoPhaseLockRecord
  - TWOPHASE_RM_LOCK_ID
- Called from (representative examples):
  - PrepareTransaction

## Notes and Other Information
- Session-level locks are completely ignored and not transferred to the prepared transaction
- Virtual transaction (VXID) locks are excluded as they are not meaningful after a database restart
- Fast-path locks are converted to regular lock table entries to ensure proper 2PC handling
- The holdsStrongLockCount flag is cleared to prevent premature strong lock count decrements
- Each qualifying lock generates a TwoPhaseLockRecord that will be processed during COMMIT/ROLLBACK PREPARED
- This function is part of the two-phase commit protocol implementation for maintaining lock consistency across transaction boundaries