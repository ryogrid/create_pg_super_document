# heap_lock_updated_tuple

## Location
[src/backend/access/heap/heapam.c:5997-6041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L5997-L6041)

## Overview
heap_lock_updated_tuple follows and locks all tuples in an update chain after the initial tuple, ensuring consistent locking across tuple versions without checking visibility.

## Definition
```c
static TM_Result heap_lock_updated_tuple(Relation rel, HeapTuple tuple, ItemPointer ctid,
                                          TransactionId xid, LockTupleMode mode)
```

## Detailed Description
This static function serves as the entry point for locking updated versions of a tuple in an update chain. Unlike heap_lock_tuple, this function assumes the initial tuple is already locked and focuses on acquiring locks on subsequent versions in the update chain.

The function performs a lightweight check to determine if there are actually updated versions to lock by comparing the tuple's t_self with the provided ctid pointer. If they differ and the tuple hasn't moved to another partition, it means there are updated versions that need to be locked.

A critical optimization implemented here is the MultiXactId management: before beginning the potentially complex locking process, it calls MultiXactIdSetOldestMember() to establish the transaction's MultiXact membership baseline. This ensures that even if the current operation only uses a simple TransactionId, the system is prepared for other backends to potentially incorporate this transaction into MultiXactIds.

The function delegates the actual recursive locking work to heap_lock_updated_tuple_rec, maintaining a clean separation between the entry-point logic and the complex recursive implementation.

## Parameters / Member Variables
- `rel`: Relation containing the tuple and its update chain
- `tuple`: HeapTuple representing the base tuple (assumed already locked)
- `ctid`: ItemPointer to the continuation point in the update chain
- `xid`: TransactionId of the transaction requesting locks
- `mode`: LockTupleMode specifying the strength of locks to acquire

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderIndicatesMovedPartitions
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - [MultiXactIdSetOldestMember](../M/MultiXactIdSetOldestMember.md)
  - [heap_lock_updated_tuple_rec](heap_lock_updated_tuple_rec.md)
- Called from (representative examples):
  - [heap_lock_tuple](heap_lock_tuple.md)

## Notes and Other Information
- This is a static function internal to heapam.c, serving as a controlled entry point for update chain locking
- Does not perform visibility checks, operating under the assumption that visibility has already been verified
- Implements a key optimization by avoiding heavyweight tuple locks during the chain-walking process
- The design prevents potential starvation scenarios in complex update chain locking situations
- Assumes specific transaction isolation level constraints (not repeatable read or serializable for chain walkers)
- Returns TM_Ok immediately if no updated versions exist, avoiding unnecessary processing
- Critical for maintaining lock consistency across tuple update chains in PostgreSQL's MVCC system
- The MultiXactId preparation step ensures proper concurrency control even when the operation might use only simple TransactionIds
- Designed specifically for scenarios where the snapshot predates updates but transaction isolation allows continued processing