# PREDICATELOCKTAG

## Location
[src/include/storage/predicate_internals.h:302-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L302-L306)

## Overview
PREDICATELOCKTAG uniquely identifies an individual predicate lock by combining a lockable target object with the serializable transaction that holds the lock.

## Definition

```c
typedef struct PREDICATELOCKTAG
{
	PREDICATELOCKTARGET *myTarget;
	SERIALIZABLEXACT *myXact;
} PREDICATELOCKTAG;
```
## Detailed Description
PREDICATELOCKTAG serves as a unique identifier for individual predicate locks within PostgreSQL's serializable isolation implementation. It establishes the relationship between a specific lockable database object (represented by PREDICATELOCKTARGET) and the serializable transaction (SERIALIZABLEXACT) that has acquired a predicate lock on that object. This combination ensures that each predicate lock can be uniquely identified and managed within the system's hash tables and data structures. The tag is essential for lock lookup, conflict detection, and cleanup operations during transaction processing.

## Parameters / Member Variables
- : Pointer to the PREDICATELOCKTARGET structure representing the database object being locked
- : Pointer to the SERIALIZABLEXACT structure representing the transaction holding the lock

## Dependencies
- Functions called/Symbols referenced:
  - [PREDICATELOCKTARGET](PREDICATELOCKTARGET.md)
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md)
- Called from (representative examples):
  - [predicatelock_hash](../p/predicatelock_hash.md)
  - [CreatePredicateLock](../C/CreatePredicateLock.md)
  - [DeleteChildTargetLocks](../D/DeleteChildTargetLocks.md)
  - [TransferPredicateLocksToNewTarget](../T/TransferPredicateLocksToNewTarget.md)
  - [CheckTargetForConflictsIn](../C/CheckTargetForConflictsIn.md)
  - [ClearOldPredicateLocks](../C/ClearOldPredicateLocks.md)

## Notes and Other Information
- Used as a hash key in predicate lock hash tables for efficient lookup operations
- Critical component in serializable snapshot isolation conflict detection algorithms
- Enables bidirectional navigation between targets and transactions in predicate locking system
- Lifetime tied to the duration of the predicate lock - created when lock is acquired, destroyed when lock is released
- [Hash](../H/Hash.md) function predicatelock_hash() operates on this structure for hash table operations