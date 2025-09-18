# ExecRowMark

## Location
src/include/nodes/execnodes.h: 750 - 763

## Overview
ExecRowMark is the runtime representation of FOR [KEY] UPDATE/SHARE clauses, managing row locking information during query execution.

## Definition


## Detailed Description
ExecRowMark manages row locking during the execution of queries with FOR [KEY] UPDATE/SHARE clauses. It maintains the runtime state necessary to apply proper locking semantics, including lock strength, wait policies, and tracking of currently locked tuples. The structure supports both regular relations and virtual relations (like subqueries), with special handling for inheritance hierarchies. Each non-target relation in a locking query gets its own ExecRowMark entry stored in the EState's es_rowmarks array.

## Parameters / Member Variables
- : Pointer to the opened and appropriately locked Relation
- : Object identifier of the relation (InvalidOid for subqueries)
- : Range table index identifying this relation in the query's range table
- : Parent range table index when this is a child in an inheritance hierarchy
- : Unique identifier used for resjunk columns related to this row mark
- : Type of row marking from RowMarkType enum (e.g., ROW_MARK_EXCLUSIVE, ROW_MARK_SHARE)
- : Lock strength from LockClauseStrength enum or LCS_NONE if not from a locking clause
- : Wait policy for lock acquisition (NOWAIT, SKIP LOCKED, etc.)
- : Boolean flag indicating if this mark is relevant for the current tuple being processed
- : Current tuple identifier (ctid) of the currently locked tuple, used by WHERE CURRENT OF
- : Generic pointer available for use by the plan node sourcing this relation (e.g., FDW-specific data)

## Dependencies
- Functions called/Symbols referenced:
  - RowMarkType (enum type)
  - LockClauseStrength (enum type)
  - LockWaitPolicy (enum type)
- Called from (representative examples):
  - InitPlan (src/backend/executor/execMain.c:855)
  - ExecFindRowMark (src/backend/executor/execMain.c:2384)
  - ExecLockRows (src/backend/executor/nodeLockRows.c:78)

## Notes and Other Information
ExecRowMark is essential for implementing PostgreSQL's multi-version concurrency control (MVCC) locking semantics. It works closely with the row locking infrastructure to ensure proper isolation levels are maintained during concurrent access. The structure is designed to handle complex scenarios including inheritance hierarchies, foreign tables (via ermExtra), and various lock strengths and wait policies. Virtual relations like subqueries have a NULL relation pointer but still participate in the locking protocol for consistency.