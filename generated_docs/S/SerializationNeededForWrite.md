# SerializationNeededForWrite

## Location
[src/backend/storage/lmgr/predicate.c:560-581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L560-L581)

## Overview
Determines whether serialization conflict detection is needed for write operations in a serializable transaction, providing a simpler counterpart to SerializationNeededForRead.

## Definition
```c
static inline bool SerializationNeededForWrite(Relation relation)
```

## Detailed Description
This function serves as the write-side equivalent of SerializationNeededForRead, determining whether a write operation requires predicate locking as part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation. It performs a simplified version of the read-side checks:

1. **Transaction check**: Returns false if not in a serializable transaction  
2. **Relation filtering**: Delegates to PredicateLockingNeededForRelation to check if the relation needs predicate locking

Unlike the read version, this function is simpler because:
- Write operations don't involve snapshots, so no snapshot type checking is needed
- Write transactions cannot be RO-safe (read-only safe), so no RO-safe optimization applies
- The function has no side effects, unlike its read counterpart

The function is used to guard write-side serialization conflict detection, ensuring that only eligible relations in serializable transactions participate in the SSI protocol.

## Parameters / Member Variables
- `relation`: The Relation structure for the table/index being written to

## Dependencies
- Functions called/Symbols referenced:
  - InvalidSerializableXact (constant indicating no active serializable transaction)
  - [PredicateLockingNeededForRelation](../P/PredicateLockingNeededForRelation.md) (function to check relation eligibility)
  - [SERIALIZABLEXACT](SERIALIZABLEXACT.md) (referenced in context but not directly called)
- Called from (representative examples):
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [CheckTableForSerializableConflictIn](../C/CheckTableForSerializableConflictIn.md)

## Notes and Other Information
- This function is marked inline for performance since it's called during write operations
- Simpler than SerializationNeededForRead due to write-specific characteristics
- No side effects, unlike the read version which can release locks
- Part of the write-side serialization conflict detection in PostgreSQL's SSI implementation
- Works with CheckForSerializableConflictIn functions to detect dangerous write patterns that could cause serialization anomalies