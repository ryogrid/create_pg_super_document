# predicatelock_twophase_recover

## Location
src/backend/storage/lmgr/predicate.c: 4899 - 5035

## Overview
Reconstructs serializable transaction state and predicate locks during recovery from two-phase commit state file records.

## Definition
```c
void predicatelock_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
```

## Detailed Description
This function is the recovery counterpart to AtPrepare_PredicateLocks, responsible for reconstructing the serializable transaction state from 2PC state file records during database recovery. It processes two types of records created during prepare:

**Transaction Record Recovery (TWOPHASEPREDICATERECORD_XACT):**
- Creates a new SERIALIZABLEXACT structure using CreatePredXact()
- Sets up the transaction with a special vxid (INVALID_PROC_NUMBER/xid) since no actual process is associated during recovery
- Initializes conflict lists and conservatively assumes the transaction had both incoming and outgoing conflicts by setting summary conflict flags
- Updates global transaction state including SxactGlobalXmin and WritableSxactCount
- Registers the transaction in SerializableXidHash for later lookup

**Lock Record Recovery (TWOPHASEPREDICATERECORD_LOCK):**
- Recreates individual predicate locks by calling CreatePredicateLock()
- Looks up the previously recovered SERIALIZABLEXACT by transaction ID
- Associates each recovered lock with the correct serializable transaction

The conservative approach during recovery ensures serialization safety by assuming conflicts existed even if they weren't explicitly recorded, since conflicts can be added after preparation.

## Parameters / Member Variables
- `xid`: Transaction ID of the prepared transaction being recovered
- `info`: Additional information from 2PC record (currently unused)
- `recdata`: Pointer to the TwoPhasePredicateRecord containing serialized state
- `len`: Length of the record data (validated to match expected size)

## Dependencies
- Functions called/Symbols referenced:
  - CreatePredXact: Creates new SERIALIZABLEXACT structure
  - CreatePredicateLock: Recreates individual predicate locks
  - PredicateLockTargetTagHashCode: Computes hash for lock targets
  - hash_search: Searches/inserts into SerializableXidHash
  - SerialSetActiveSerXmin: Updates global minimum transaction ID
  - TransactionIdFollows/TransactionIdEquals: Transaction ID comparison utilities
  - dlist_init/dlist_node_init: Initialize list structures
  - LWLockAcquire/LWLockRelease: Lock management
- Called from (representative examples):
  - Two-phase commit recovery system during database startup

## Notes and Other Information
- Handles both transaction and lock record types within a single function
- Uses conservative conflict assumptions (SXACT_FLAG_SUMMARY_CONFLICT_IN/OUT) to ensure safety during recovery
- Special handling for global xmin updates during recovery allows backwards movement
- Prepared transactions during recovery have no associated process (pid = 0, pgprocno = INVALID_PROC_NUMBER)
- Critical for maintaining serializable isolation across database restarts when prepared transactions exist
- The function includes assertions to validate record structure and transaction state consistency