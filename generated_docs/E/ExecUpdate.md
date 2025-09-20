# ExecUpdate

## Location
[src/backend/executor/nodeModifyTable.c:2292-2543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2292-L2543)

## Overview
ExecUpdate is the main function responsible for executing UPDATE operations in PostgreSQL, handling various update scenarios including regular table updates, foreign table updates, view updates, and cross-partition updates.

## Definition

```c
static TupleTableSlot *
ExecUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
		   bool canSetTag)
```
## Detailed Description
ExecUpdate orchestrates the complete UPDATE operation workflow in PostgreSQL. The function handles multiple update scenarios based on the target relation type:

1. **Bootstrap Mode Validation**: Prevents updates during database bootstrap
2. **Update Preparation**: Calls ExecUpdatePrologue to handle BEFORE ROW triggers and validation
3. **INSTEAD OF Trigger Handling**: For view updates, executes INSTEAD OF ROW UPDATE triggers
4. **Foreign Table Updates**: Delegates to FDW-specific update routines for foreign tables
5. **Regular Table Updates**: Performs the core update logic including:
   - Tuple locking and concurrency control via ExecUpdateAct
   - EvalPlanQual (EPQ) processing for concurrent updates
   - Cross-partition update handling
   - Tuple versioning and conflict resolution
6. **Post-Update Processing**: Calls ExecUpdateEpilogue for index maintenance and AFTER triggers
7. **RETURNING Clause Processing**: Handles RETURNING expressions if present

The function implements PostgreSQL's Multi-Version Concurrency Control (MVCC) semantics and handles various tuple modification states (TM_Ok, TM_Updated, TM_Deleted, TM_SelfModified).

## Parameters / Member Variables
- : ModifyTableContext containing execution state, EPQ state, and command metadata
- : ResultRelInfo for the target relation being updated
- : ItemPointer identifying the tuple to update (invalid for foreign tables and view triggers)
- : HeapTuple containing original data (for view triggers and foreign tables)
- : TupleTableSlot containing the new tuple values
- : Boolean indicating whether the processed tuple count can be incremented

## Dependencies
- Functions called/Symbols referenced:
  - [ExecUpdatePrologue](ExecUpdatePrologue.md)
  - [ExecIRUpdateTriggers](ExecIRUpdateTriggers.md)  
  - [ExecUpdatePrepareSlot](ExecUpdatePrepareSlot.md)
  - [ExecUpdateAct](ExecUpdateAct.md)
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md)
  - [EvalPlanQualSlot](EvalPlanQualSlot.md)
  - [EvalPlanQual](EvalPlanQual.md)
  - [ExecInitUpdateProjection](ExecInitUpdateProjection.md)
  - [ExecGetUpdateNewTuple](ExecGetUpdateNewTuple.md)
  - [ExecProcessReturning](ExecProcessReturning.md)
  - table_tuple_lock
  - table_tuple_fetch_row_version
  - IsolationUsesXactSnapshot
- Called from (representative examples):
  - [ExecOnConflictUpdate](ExecOnConflictUpdate.md) (src/backend/executor/nodeModifyTable.c:2746)
  - [ExecModifyTable](ExecModifyTable.md) (src/backend/executor/nodeModifyTable.c:4316)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Cross-partition updates are transparently handled and may return early with special RETURNING slot processing
- [EvalPlanQual](EvalPlanQual.md) processing ensures snapshot isolation by re-evaluating plan conditions for concurrently modified tuples
- The function includes comprehensive error handling for concurrent modifications and serialization failures
- For foreign tables, the FDW is responsible for determining which row to update using plan slot data
- INSTEAD OF triggers are used for view updates where the view itself is not directly updatable
- The redo_act label enables retry logic when EPQ determines a new tuple version should be processed
- Bootstrap mode restrictions prevent infinite update loops during database initialization