# RWConflict

## Location
[src/include/storage/predicate_internals.h:201-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L201-L202)

## Overview
RWConflict is a typedef that defines a pointer to RWConflictData, providing a convenient handle for managing read-write conflict relationships between serializable transactions.

## Definition
```c
typedef struct RWConflictData *RWConflict;
```

## Detailed Description
RWConflict is a simple pointer typedef that abstracts access to RWConflictData structures. This typedef is used extensively throughout PostgreSQL's predicate locking system to manage and manipulate conflict relationships between serializable transactions. It provides type safety and code clarity when working with conflict detection and resolution in the Serializable Snapshot Isolation implementation.

The typedef enables clean function signatures and variable declarations when dealing with conflict management operations such as creation, lookup, and cleanup of read-write conflicts.

## Parameters / Member Variables
This is a typedef for a pointer, so it has no direct member variables. It points to an RWConflictData structure which contains:
- List links for conflict management
- Pointers to the conflicting serializable transactions
- Conflict relationship information

## Dependencies
- Functions called/Symbols referenced:
  - [RWConflictData](RWConflictData.md)
- Called from (representative examples):
  - [RWConflictExists](RWConflictExists.md)
  - [SetRWConflict](../S/SetRWConflict.md)
  - [SetPossibleUnsafeConflict](../S/SetPossibleUnsafeConflict.md)
  - [ReleaseRWConflict](ReleaseRWConflict.md)
  - [OnConflict_CheckForSerializationFailure](../O/OnConflict_CheckForSerializationFailure.md)

## Notes and Other Information
- Essential typedef for PostgreSQL's SSI implementation
- Provides type-safe access to conflict relationship data
- Used extensively in conflict detection and serialization failure analysis
- Enables clean abstraction of pointer-based access to conflict structures
- Critical component for managing transaction dependency graphs
- Part of the memory management system for conflict tracking
- Facilitates efficient conflict creation, lookup, and cleanup operations