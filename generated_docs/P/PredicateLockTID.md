# PredicateLockTID

## Location
src/backend/storage/lmgr/predicate.c: 2611 - 2658

## Overview
Acquires a predicate lock at the tuple level for serializable transaction isolation, preventing read-write conflicts by tracking tuple-level reads during serializable transactions.

## Definition


## Detailed Description
PredicateLockTID is a core component of PostgreSQL's serializable snapshot isolation (SSI) implementation. It establishes a predicate lock on a specific tuple (identified by its TID - tuple identifier) to detect potential serialization conflicts. The function performs several optimizations:

1. **Early exits**: Returns immediately if serialization is not needed for the current transaction/relation combination or if operating on temporary tables
2. **Write conflict detection**: Skips locking if the current transaction already wrote the tuple (avoiding self-conflicts)  
3. **Lock hierarchy optimization**: Checks for existing relation-level locks before acquiring tuple-level locks, as relation locks subsume tuple locks
4. **Efficient targeting**: Uses precise tuple identification through database OID, relation OID, block number, and offset number

The function is essential for maintaining consistency in SERIALIZABLE isolation level by ensuring that reads are tracked and can be checked against concurrent writes.

## Parameters / Member Variables
- : The relation containing the tuple to be locked
- : ItemPointer identifying the specific tuple (block number + offset number)
- : The snapshot under which the read is occurring
- : Transaction ID that created/last modified the tuple

## Dependencies
- Functions called/Symbols referenced:
  - SerializationNeededForRead
  - TransactionIdIsCurrentTransactionId  
  - SET_PREDICATELOCKTARGETTAG_RELATION
  - PredicateLockExists
  - SET_PREDICATELOCKTARGETTAG_TUPLE
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
  - PredicateLockAcquire
- Called from (representative examples):
  - heap_fetch
  - heap_hot_search_buffer
  - heapam_scan_bitmap_next_block

## Notes and Other Information
- Only active when running under SERIALIZABLE isolation level
- Implements part of PostgreSQL's Serializable Snapshot Isolation (SSI) algorithm
- Performs lock hierarchy checks to avoid redundant locking when coarser-grained locks exist
- Critical for detecting rw-conflicts (read-write conflicts) in serializable transactions
- Part of the predicate locking subsystem that prevents serialization anomalies