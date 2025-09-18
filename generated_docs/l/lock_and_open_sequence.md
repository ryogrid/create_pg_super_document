# lock_and_open_sequence

## Location
[src/backend/commands/sequence.c:1085-1112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1085-L1112)

## Overview
Locks and opens a sequence relation, ensuring proper transaction-level locking for sequence operations.

## Definition
```c
static Relation lock_and_open_sequence(SeqTable seq)
```

## Detailed Description
This static function manages the locking and opening of sequence relations in PostgreSQL. It implements a transaction-level caching mechanism to avoid acquiring locks multiple times within the same transaction. The function ensures that sequence locks are owned by the top transaction rather than subtransactions, which optimizes lock management for sequences accessed multiple times in a single transaction.

The function checks if a lock has already been acquired for the sequence in the current transaction by comparing the local transaction ID. If no lock exists, it temporarily switches the resource owner to the top transaction's resource owner, acquires a RowExclusiveLock on the sequence relation, and then restores the original resource owner.

## Parameters / Member Variables
- `seq`: A SeqTable entry containing sequence metadata including the relation OID and last transaction ID that locked it

## Dependencies
- Functions called/Symbols referenced:
  - SeqTable (sequence table entry structure)
  - LocalTransactionId (transaction identifier type)
  - ResourceOwner (resource ownership management)
  - [LockRelationOid](../L/LockRelationOid.md) (acquires lock on relation by OID)
  - [sequence_open](../s/sequence_open.md) (opens sequence relation)
- Called from (representative examples):
  - [lastval](lastval.md)
  - [init_sequence](../i/init_sequence.md)

## Notes and Other Information
- The function uses RowExclusiveLock to ensure exclusive access for sequence modifications
- Lock ownership is transferred to the top transaction to avoid lock escalation issues in subtransactions
- The lxid field in SeqTable is used to track which transaction last acquired the lock
- This is a static function internal to src/backend/commands/sequence.c