# table_tuple_lock

## Location
[src/include/access/tableam.h:1581-1595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1581-L1595)

## Overview
Locks a tuple in a specified mode for concurrent access control, supporting various locking strategies and update chain following capabilities.

## Definition
```c
static inline TM_Result
table_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
                 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
                 LockWaitPolicy wait_policy, uint8 flags,
                 TM_FailureData *tmfd)
```

## Detailed Description
This function provides the core interface for acquiring locks on individual tuples within a table. It supports different lock modes and wait policies to handle concurrent access scenarios. The function can follow update chains to lock descendant tuples or find the latest version of a tuple depending on the specified flags.

Key capabilities include:
- Multiple lock modes for different concurrency requirements
- Configurable wait policies (wait, skip, or error on conflict)
- Update chain following to lock related tuples
- Latest version location for multi-version concurrency control
- Integration with MVCC visibility rules
- Comprehensive failure reporting through TM_FailureData

The function serves as a wrapper around the table access method's tuple_lock implementation, allowing different storage engines to provide their own locking mechanisms while maintaining interface consistency.

## Parameters / Member Variables
- `rel`: The relation containing the tuple to lock (caller must hold suitable lock)
- `tid`: ItemPointer (TID) identifying the tuple to lock
- `snapshot`: Snapshot for visibility determinations
- `slot`: Output parameter that will contain the target tuple
- `cid`: Current command ID for visibility testing and stored in tuple's cmax if successful
- `mode`: The desired lock mode (e.g., LockTupleShared, LockTupleExclusive)
- `wait_policy`: Policy for handling lock conflicts (wait, skip, error)
- `flags`: Control flags for special behaviors:
  - TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS: Follow update chain to lock descendants
  - TUPLE_LOCK_FLAG_FIND_LAST_VERSION: Follow update chain to lock latest version
- `tmfd`: Output parameter filled with failure details when locking fails

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->tuple_lock (table access method function pointer)
- Types referenced:
  - CommandId
  - LockTupleMode
  - LockWaitPolicy
  - TM_FailureData
- Called from (representative examples):
  - GetTupleForTrigger (in src/backend/commands/trigger.c:3400)
  - RelationFindReplTupleByIndex (in src/backend/executor/execReplication.c:253)
  - RelationFindReplTupleSeq (in src/backend/executor/execReplication.c:437)
  - ExecLockRows (in src/backend/executor/nodeLockRows.c:185)
  - ExecDelete (in src/backend/executor/nodeModifyTable.c:1589)
  - ExecUpdate (in src/backend/executor/nodeModifyTable.c:2426)
  - ExecOnConflictUpdate (in src/backend/executor/nodeModifyTable.c:2580)
  - ExecMergeMatched (in src/backend/executor/nodeModifyTable.c:3210)

## Notes and Other Information
- Return values include TM_Ok (success), TM_Invisible, TM_SelfModified, TM_Updated, TM_Deleted, and TM_WouldBlock
- On successful lock acquisition (TM_Ok), the target tuple is loaded into the provided slot
- For failures other than TM_Invisible and TM_Deleted, tmfd is filled with tuple's t_ctid, t_xmax, and t_cmax when available
- The function is widely used throughout the executor for various operations requiring tuple locking
- Proper relation locking is the caller's responsibility
- Lock modes and wait policies must be chosen carefully based on the specific concurrency requirements
- Update chain following flags enable complex locking scenarios for MVCC environments