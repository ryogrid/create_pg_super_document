# sequence_close

## Location
src/backend/access/sequence/sequence.c: 58 - 69

## Overview
Closes a sequence relation and optionally releases the specified lock held on it.

## Definition
```c
void sequence_close(Relation relation, LOCKMODE lockmode)
```

## Detailed Description
The `sequence_close` function is a wrapper around `relation_close` specifically designed for sequence relations. It closes the given sequence relation and manages the lock release according to the specified lock mode. If the lockmode is not "NoLock", the function releases the specified lock on the relation.

The function follows PostgreSQL's standard pattern for resource cleanup, ensuring that sequence relations are properly closed and their locks are managed appropriately. It's worth noting that in many cases it's sensible to hold locks beyond the relation close operation, in which case the locks are automatically released at transaction end.

## Parameters / Member Variables
- `relation`: The sequence relation to close (previously opened with sequence_open)
- `lockmode`: The lock mode to release; if NoLock, no lock is released

## Dependencies
- Functions called/Symbols referenced:
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [DefineSequence](../D/DefineSequence.md)
  - [ResetSequence](../R/ResetSequence.md)
  - [AlterSequence](../A/AlterSequence.md)
  - [SequenceChangePersistence](../S/SequenceChangePersistence.md)
  - [nextval_internal](../n/nextval_internal.md)
  - [currval_oid](../c/currval_oid.md)
  - [lastval](../l/lastval.md)
  - [do_setval](../d/do_setval.md)
  - [pg_sequence_last_value](../p/pg_sequence_last_value.md)

## Notes and Other Information
- This function is the counterpart to `sequence_open` and should be used to properly clean up sequence relations
- Lock management is flexible - locks can be held beyond the close operation and will be automatically released at transaction end
- Located in src/backend/access/sequence/sequence.c as part of the sequence access subsystem
- The function is widely used throughout the sequence command implementation, indicating its importance in proper resource management