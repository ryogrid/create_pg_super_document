# RWConflictData

## Location
[src/include/storage/predicate_internals.h:193-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L193-L199)

## Overview
RWConflictData represents a read-write conflict relationship between two serializable transactions, serving as a fundamental building block for detecting serialization anomalies in PostgreSQL's SSI implementation.

## Definition
```c
typedef struct RWConflictData
{
    dlist_node outLink;
    dlist_node inLink;
    SERIALIZABLEXACT *sxactOut;
    SERIALIZABLEXACT *sxactIn;
} RWConflictData;
```

## Detailed Description
RWConflictData structures track dependencies between pairs of serializable transactions where one transaction reads data that another transaction has written or will write. These conflicts are essential for detecting dangerous dependency cycles that could lead to serialization anomalies. The structure supports efficient list management through embedded list nodes and can represent both actual conflicts and potential unsafe relationships for read-only transaction optimization.

When not actively tracking a conflict, these structures are maintained on an available list for reuse, with the outLink field serving as the list maintenance mechanism.

## Parameters / Member Variables
- `outLink`: List node for maintaining conflicts outbound from a transaction (also used for available list when not in use)
- `inLink`: List node for maintaining conflicts inbound to a transaction
- `sxactOut`: Pointer to the serializable transaction that has the outbound conflict (the writer)
- `sxactIn`: Pointer to the serializable transaction that has the inbound conflict (the reader)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](../d/dlist_node.md)
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md)
- Called from (representative examples):
  - [RWConflictExists](RWConflictExists.md)
  - [SetRWConflict](../S/SetRWConflict.md)
  - [SetPossibleUnsafeConflict](../S/SetPossibleUnsafeConflict.md)
  - [OnConflict_CheckForSerializationFailure](../O/OnConflict_CheckForSerializationFailure.md)

## Notes and Other Information
- Central to PostgreSQL's implementation of Serializable Snapshot Isolation
- Used for both actual read-write conflicts and potential unsafe relationships
- Enables efficient cycle detection in transaction dependency graphs
- Memory management through reusable pool maintained on available list
- Critical for determining when serialization failures must be reported
- Supports both conflict tracking and safe snapshot identification for read-only transactions
- Part of the core mechanism preventing write skew and other serialization anomalies