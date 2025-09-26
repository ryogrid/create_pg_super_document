# AtPrepare_PredicateLocks

## Location
[src/backend/storage/lmgr/predicate.c:4780-4848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4780-L4848)

## Overview
Prepares predicate locks for two-phase commit by serializing the current transaction's serializable state and all held predicate locks into 2PC state file records.

## Definition
```c
void AtPrepare_PredicateLocks(void)
```

## Detailed Description
This function is part of PostgreSQL's two-phase commit (2PC) support for serializable transactions. When a transaction is being prepared for 2PC, this function creates persistent records of the transaction's serializable state and all predicate locks it currently holds. This information is stored in the 2PC state file to ensure that the serializable isolation guarantees can be maintained across the prepare/commit phases.

The function creates two types of records:
1. **Transaction record (TWOPHASEPREDICATERECORD_XACT)**: Contains the serializable transaction's metadata including xmin and flags, but deliberately excludes conflict information since new conflicts can arise even after preparation.
2. **Lock records (TWOPHASEPREDICATERECORD_LOCK)**: One record for each predicate lock held by the transaction, containing the lock target information.

The function walks through the transaction's predicate lock list rather than using the local predicate lock table to ensure accuracy. All records are registered with the two-phase commit system using the TWOPHASE_RM_PREDICATELOCK_ID resource manager identifier.

## Parameters / Member Variables
This function takes no parameters and operates on the current transaction's serializable state.

## Dependencies
- Functions called/Symbols referenced:
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md): Registers 2PC state records for persistence
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Manages SerializablePredicateListLock
  - dlist_foreach/dlist_container: Iterates through predicate locks
  - IsParallelWorker/ParallelContextActive: Parallel execution checks
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md): During two-phase commit preparation

## Notes and Other Information
- Only creates records if the current transaction is actually serializable (MySerializableXact != InvalidSerializableXact)
- Deliberately excludes conflict information from the transaction record since conflicts can be added after preparation
- Uses conservative assumptions during recovery to handle missing conflict information
- Requires SerializablePredicateListLock in shared mode to safely iterate predicate locks
- Includes assertions to ensure it's not called in parallel worker contexts during preparation
- The TWOPHASE_RM_PREDICATELOCK_ID resource manager handles recovery of these records