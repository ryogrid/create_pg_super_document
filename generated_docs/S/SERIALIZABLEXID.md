# SERIALIZABLEXID

## Location
[src/include/storage/predicate_internals.h:240-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L240-L247)

## Overview
A hash table entry structure that provides a mapping from a TransactionId to the corresponding SERIALIZABLEXACT record for serializable transactions in PostgreSQL's predicate locking system.

## Definition

```c
typedef struct SERIALIZABLEXID
{
	/* hash key */
	SERIALIZABLEXIDTAG tag;

	/* data */
	SERIALIZABLEXACT *myXact;	/* pointer to the top level transaction data */
} SERIALIZABLEXID;
```
## Detailed Description
SERIALIZABLEXID serves as a bridge structure in PostgreSQL's serializable snapshot isolation system, linking transaction IDs to their corresponding serializable transaction records. This structure is essential for quickly locating SERIALIZABLEXACT data structures when given just a transaction ID. The entries are created when top-level transaction IDs are first assigned to transactions participating in predicate locking, though this may never happen for read-only transactions. The structure persists even after a transaction completes and its connection closes, ensuring that serialization conflict detection can continue to function properly. The SubTransGetTopmostTransaction method is used when necessary to map subtransaction XIDs to their top-level transaction XIDs.

## Parameters / Member Variables
- : A SERIALIZABLEXIDTAG structure containing the transaction ID used as the hash key for lookups
- : A pointer to the SERIALIZABLEXACT structure containing the complete transaction data for the top-level transaction

## Dependencies
- Functions called/Symbols referenced:
  - [SERIALIZABLEXIDTAG](SERIALIZABLEXIDTAG.md) (hash key structure containing TransactionId)
  - [SERIALIZABLEXACT](SERIALIZABLEXACT.md) (target transaction data structure)
- Called from (representative examples):
  - [InitPredicateLocks](../I/InitPredicateLocks.md) (predicate locking system initialization)
  - [PredicateLockShmemSize](../P/PredicateLockShmemSize.md) (shared memory size calculation)
  - [RegisterPredicateLockingXid](../R/RegisterPredicateLockingXid.md) (transaction registration for predicate locking)
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md) (serialization conflict detection)
  - PredicateLockTwoPhaseFinish (two-phase commit handling)
  - predicatelock_twophase_recover (two-phase commit recovery)

## Notes and Other Information
- Created only for transactions that participate in predicate locking (typically write transactions)
- Provides persistence beyond transaction completion to support ongoing conflict detection
- Used in conjunction with SubTransGetTopmostTransaction to handle subtransactions properly
- Essential component of the hash table infrastructure for efficient transaction lookup
- Part of the broader serializable snapshot isolation implementation that prevents serialization anomalies
- Supports both normal transaction processing and two-phase commit scenarios
- Memory management is handled by the predicate locking subsystem