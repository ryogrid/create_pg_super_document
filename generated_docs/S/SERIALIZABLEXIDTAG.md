# SERIALIZABLEXIDTAG

## Location
[src/include/storage/predicate_internals.h:222-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L222-L225)

## Overview
A structure that serves as a hash table tag for identifying serializable transactions by their transaction ID (xid) in PostgreSQL's serializable snapshot isolation system.

## Definition

```c
typedef struct SERIALIZABLEXIDTAG
{
	TransactionId xid;
} SERIALIZABLEXIDTAG;
```
## Detailed Description
SERIALIZABLEXIDTAG is a simple tag structure used as a key in hash tables that track serializable transactions in PostgreSQL's serializable snapshot isolation implementation. The structure contains only a transaction ID (xid) and is designed to identify a serializable transaction or any of its subtransactions. This tag structure is typically used in hash table lookups to quickly find or store information about specific serializable transactions. The serializable isolation level requires tracking relationships between transactions to detect and prevent serialization anomalies, and this tag structure provides an efficient way to index transaction-specific data structures.

## Parameters / Member Variables
- : A TransactionId (uint32) that uniquely identifies a serializable transaction or one of its subtransactions

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (typedef for uint32 from src/include/c.h)
- Called from (representative examples):
  - [InitPredicateLocks](../I/InitPredicateLocks.md) (initialization of predicate locking system)
  - [RegisterPredicateLockingXid](../R/RegisterPredicateLockingXid.md) (registering transaction for predicate locking)
  - [ReleaseOneSerializableXact](../R/ReleaseOneSerializableXact.md) (releasing serializable transaction resources)
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md) (checking for serialization conflicts)
  - PredicateLockTwoPhaseFinish (two-phase commit predicate lock handling)
  - predicatelock_twophase_recover (recovery of two-phase predicate locks)
  - [SERIALIZABLEXID](SERIALIZABLEXID.md) (related data structure)

## Notes and Other Information
- Used as a hash table key for efficient lookup of serializable transaction information
- Part of the predicate locking system that implements serializable snapshot isolation
- The simple structure design (single xid field) makes it suitable for hash table operations
- Works in conjunction with SERIALIZABLEXID structure to manage serializable transaction state
- Essential for detecting dangerous structures that could lead to serialization anomalies
- Used in both normal transaction processing and two-phase commit recovery scenarios