# sequence_open

## Location
[src/backend/access/sequence/sequence.c:37-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/sequence/sequence.c#L37-L57)

## Overview
Opens a sequence relation by its OID and ensures the relation is a valid sequence type.

## Definition

```c
Relation
sequence_open(Oid relationId, LOCKMODE lockmode)
```
## Detailed Description
The  function is a specialized wrapper around  that provides type-safe access to sequence relations. It opens a relation by its object identifier (OID) with the specified lock mode and validates that the opened relation is indeed a sequence. This function ensures that sequence operations are only performed on actual sequence objects, preventing runtime errors and maintaining data integrity.

The function follows PostgreSQL's standard pattern of opening relations with appropriate locking and validation, specifically tailored for sequence access patterns.

## Parameters / Member Variables
- `relationId`: The object identifier (OID) of the sequence relation to open
- `lockmode`: The lock mode to acquire on the relation (e.g., AccessShareLock, RowExclusiveLock)
## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [validate_relation_kind](../v/validate_relation_kind.md)
- Called from (representative examples):
  - [DefineSequence](../D/DefineSequence.md)
  - [lock_and_open_sequence](../l/lock_and_open_sequence.md)

## Notes and Other Information
- This function is part of the sequence access API and should be used instead of direct  calls when working with sequences
- The validation step ensures type safety and prevents operations on non-sequence relations
- Located in src/backend/access/sequence/sequence.c, part of the sequence access subsystem
- The function acquires the specified lock on the relation, which must be released by the caller using  or

## Simplified Source

```c
Relation sequence_open(Oid relationId, LOCKMODE lockmode) {
    Relation r;

    // Open relation with specified lock
    r = relation_open(relationId, lockmode);

    // Verify it's actually a sequence
    validate_relation_kind(r);

    return r;
}
``` 