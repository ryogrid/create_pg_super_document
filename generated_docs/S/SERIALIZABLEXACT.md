# SERIALIZABLEXACT

## Location
src/include/storage/predicate_internals.h: 58 - 119

## Overview
SERIALIZABLEXACT is a core data structure that tracks information for each serializable database transaction to support Serializable Snapshot Isolation (SSI) techniques in PostgreSQL.

## Definition
```c
typedef struct SERIALIZABLEXACT
{
    VirtualTransactionId vxid;
    SerCommitSeqNo prepareSeqNo;
    SerCommitSeqNo commitSeqNo;
    union
    {
        SerCommitSeqNo earliestOutConflictCommit;
        SerCommitSeqNo lastCommitBeforeSnapshot;
    } SeqNo;
    dlist_head outConflicts;
    dlist_head inConflicts;
    dlist_head predicateLocks;
    dlist_node finishedLink;
    dlist_node xactLink;
    LWLock perXactPredicateListLock;
    dlist_head possibleUnsafeConflicts;
    TransactionId topXid;
    TransactionId finishedBefore;
    TransactionId xmin;
    uint32 flags;
    int pid;
    int pgprocno;
} SERIALIZABLEXACT;
```

## Detailed Description
The SERIALIZABLEXACT structure maintains comprehensive state information for serializable transactions. It is allocated when a serializable transaction acquires a snapshot and persists until all concurrent transactions complete (with optimizations for READ ONLY transactions). The structure tracks transaction ordering through sequence numbers, manages read-write conflicts, and maintains predicate locks to detect serialization anomalies.

The structure uses a home-grown shared memory list management system and supports both regular and two-phase commit scenarios. Transaction cleanup eligibility is determined by comparing the finishedBefore field to SxactGlobalXmin.

## Parameters / Member Variables
- `vxid`: Virtual transaction ID of the executing process
- `prepareSeqNo`: Sequence number assigned when transaction is marked as prepared during commit
- `commitSeqNo`: Sequence number assigned when transaction is marked as committed
- `SeqNo.earliestOutConflictCommit`: Used when committed with outbound conflicts
- `SeqNo.lastCommitBeforeSnapshot`: Used when not committed or no outbound conflicts
- `outConflicts`: List of write transactions whose data this transaction could not read
- `inConflicts`: List of read transactions that could not see this transaction's writes
- `predicateLocks`: List of associated PREDICATELOCK objects
- `finishedLink`: List link in FinishedSerializableTransactions
- `xactLink`: Link in PredXact->activeList or availableList
- `perXactPredicateListLock`: LWLock protecting predicate lock list in parallel queries
- `possibleUnsafeConflicts`: List of potentially conflicting concurrent transactions
- `topXid`: Top-level transaction ID (invalid if none exists)
- `finishedBefore`: Transaction expiration marker (invalid means still running)
- `xmin`: Transaction's snapshot xmin value
- `flags`: OR'd combination of status flags
- `pid`: Process ID of associated backend process
- `pgprocno`: PGPROC array index of associated process

## Dependencies
- Functions called/Symbols referenced:
  - [VirtualTransactionId](../V/VirtualTransactionId.md)
  - SerCommitSeqNo
  - [dlist_head](../d/dlist_head.md)
  - [dlist_node](../d/dlist_node.md)
  - [LWLock](../L/LWLock.md)
  - TransactionId
- Called from (representative examples):
  - [CreatePredXact](../C/CreatePredXact.md)
  - [ReleasePredXact](../R/ReleasePredXact.md)
  - [GetSerializableTransactionSnapshotInt](../G/GetSerializableTransactionSnapshotInt.md)
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md)

## Notes and Other Information
- Essential component of PostgreSQL's Serializable Snapshot Isolation implementation
- Memory management handled by specialized shared memory allocator
- Transaction ordering uses dual sequence numbers for robustness during commit processing
- Supports both single-phase and two-phase commit protocols
- Critical for detecting dangerous structures in transaction dependency graphs
- Performance optimizations exist for read-only transactions