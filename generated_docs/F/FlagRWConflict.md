# FlagRWConflict

## Location
[src/backend/storage/lmgr/predicate.c:4491-4525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4491-L4525)

## Overview
FlagRWConflict creates a read-write dependency between two serializable transactions and checks if this dependency causes a serialization failure.

## Definition

```c
static void
FlagRWConflict(SERIALIZABLEXACT *reader, SERIALIZABLEXACT *writer)
```
## Detailed Description
This static function is responsible for flagging read-write conflicts between serializable transactions in PostgreSQL's SSI implementation. It performs two critical operations in sequence:

1. **Serialization Failure Check**: First calls OnConflict_CheckForSerializationFailure to determine if the new conflict creates a dangerous structure that requires transaction abortion
2. **Conflict Recording**: Then records the conflict in the appropriate data structure based on the transaction states

The function handles three different conflict scenarios:
- **Summary conflict in**: When the reader is OldCommittedSxact, sets SXACT_FLAG_SUMMARY_CONFLICT_IN on the writer
- **Summary conflict out**: When the writer is OldCommittedSxact, sets SXACT_FLAG_SUMMARY_CONFLICT_OUT on the reader
- **Direct conflict**: For active transactions, calls SetRWConflict to create explicit conflict tracking

This design optimizes memory usage by using summary flags when one transaction has been committed long enough to be represented by OldCommittedSxact, while maintaining detailed conflict tracking for active transactions.

## Parameters / Member Variables
- : Pointer to the SERIALIZABLEXACT structure of the transaction that performed the read operation
- : Pointer to the SERIALIZABLEXACT structure of the transaction that performed the conflicting write operation

## Dependencies
- Functions called/Symbols referenced:
  - [OnConflict_CheckForSerializationFailure](../O/OnConflict_CheckForSerializationFailure.md)
  - [SetRWConflict](../S/SetRWConflict.md)
  - SXACT_FLAG_SUMMARY_CONFLICT_IN
  - SXACT_FLAG_SUMMARY_CONFLICT_OUT
- Called from (representative examples):
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md)
  - [CheckTargetForConflictsIn](../C/CheckTargetForConflictsIn.md)
  - [CheckTableForSerializableConflictIn](../C/CheckTableForSerializableConflictIn.md)

## Notes and Other Information
- This is a static function internal to predicate.c, part of the SSI conflict detection system
- Caller must hold LW lock on the transaction hash table before calling this function
- The function assumes reader != writer (enforced by assertion)
- Uses OldCommittedSxact optimization to reduce memory usage for conflicts with old committed transactions
- Critical path function that can trigger immediate serialization failures
- Located in src/backend/storage/lmgr/predicate.c:4491-4525

## Simplified Source

```c
static void
FlagRWConflict(SERIALIZABLEXACT *reader, SERIALIZABLEXACT *writer)
{
    Assert(reader != writer);

    // Check if this conflict causes serialization failure
    OnConflict_CheckForSerializationFailure(reader, writer);

    // Record the conflict appropriately
    if (reader == OldCommittedSxact) {
        // Reader is old committed - set summary conflict flag on writer
        writer->flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
    } else if (writer == OldCommittedSxact) {
        // Writer is old committed - set summary conflict flag on reader
        reader->flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
    } else {
        // Both transactions are active - create explicit conflict
        SetRWConflict(reader, writer);
    }
}
```