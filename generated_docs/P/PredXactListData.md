# PredXactListData

## Location
[src/include/storage/predicate_internals.h:144-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L144-L175)

## Overview
PredXactListData is a shared memory control structure that manages the global state and lists of serializable transactions for PostgreSQL's Serializable Snapshot Isolation implementation.

## Definition
```c
typedef struct PredXactListData
{
    dlist_head availableList;
    dlist_head activeList;
    TransactionId SxactGlobalXmin;
    int SxactGlobalXminCount;
    int WritableSxactCount;
    SerCommitSeqNo LastSxactCommitSeqNo;
    SerCommitSeqNo CanPartialClearThrough;
    SerCommitSeqNo HavePartialClearedThrough;
    SERIALIZABLEXACT *OldCommittedSxact;
    SERIALIZABLEXACT *element;
} PredXactListData;
```

## Detailed Description
PredXactListData serves as the central coordination structure for managing serializable transactions in shared memory. It maintains both available and active transaction lists, tracks global transaction state variables, and coordinates cleanup operations. The structure is protected by multiple locks (SerializableXactHashLock and SerializableFinishedListLock) to ensure consistent access across multiple backend processes.

The structure plays a crucial role in determining when predicate locks can be safely cleaned up and manages the global xmin for active serializable transactions, which is essential for snapshot consistency.

## Parameters / Member Variables
- `availableList`: List of available SERIALIZABLEXACT structures for reuse
- `activeList`: List of currently active serializable transactions
- `SxactGlobalXmin`: Global xmin value for all active serializable transactions
- `SxactGlobalXminCount`: Number of active serializable transactions sharing this xmin
- `WritableSxactCount`: Count of non-read-only active serializable transactions
- `LastSxactCommitSeqNo`: Monotonically increasing sequence number for serializable transaction commits
- `CanPartialClearThrough`: Sequence number threshold for safe predicate lock cleanup
- `HavePartialClearedThrough`: Sequence number indicating completed cleanup progress
- `OldCommittedSxact`: Shared copy of dummy transaction used for optimization
- `element`: Pointer to array of SERIALIZABLEXACT structures

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_head](../d/dlist_head.md)
  - TransactionId
  - SerCommitSeqNo
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md)
- Called from (representative examples):
  - [PredXactList](PredXactList.md) (typedef)
  - PredXactListDataSize (size calculation)

## Notes and Other Information
- Central control structure for SSI implementation in PostgreSQL
- Protected by multiple locks for different sections of the structure
- Critical for coordinating cleanup of predicate locks and transaction state
- Manages memory allocation pool of SERIALIZABLEXACT structures
- Global variables within this structure must be maintained consistently across all backends
- Essential for determining safe cleanup points in the serializable transaction lifecycle