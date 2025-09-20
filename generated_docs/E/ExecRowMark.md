# ExecRowMark

## Location
[src/include/nodes/execnodes.h:750-763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L750-L763)

## Overview
ExecRowMark is the runtime representation of FOR [KEY] UPDATE/SHARE clauses, managing row locking information during query execution.

## Definition

```c
typedef struct ExecRowMark
{
	Relation	relation;		/* opened and suitably locked relation */
	Oid			relid;			/* its OID (or InvalidOid, if subquery) */
	Index		rti;			/* its range table index */
	Index		prti;			/* parent range table index, if child */
	Index		rowmarkId;		/* unique identifier for resjunk columns */
	RowMarkType markType;		/* see enum in nodes/plannodes.h */
	LockClauseStrength strength;	/* LockingClause's strength, or LCS_NONE */
	LockWaitPolicy waitPolicy;	/* NOWAIT and SKIP LOCKED */
	bool		ermActive;		/* is this mark relevant for current tuple? */
	ItemPointerData curCtid;	/* ctid of currently locked tuple, if any */
	void	   *ermExtra;		/* available for use by relation source node */
} ExecRowMark;
```
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
  - [RowMarkType](../R/RowMarkType.md) (enum type)
  - LockClauseStrength (enum type)
  - LockWaitPolicy (enum type)
- Called from (representative examples):
  - [InitPlan](../I/InitPlan.md) (src/backend/executor/execMain.c:855)
  - [ExecFindRowMark](ExecFindRowMark.md) (src/backend/executor/execMain.c:2384)
  - [ExecLockRows](ExecLockRows.md) (src/backend/executor/nodeLockRows.c:78)

## Notes and Other Information
ExecRowMark is essential for implementing PostgreSQL's multi-version concurrency control (MVCC) locking semantics. It works closely with the row locking infrastructure to ensure proper isolation levels are maintained during concurrent access. The structure is designed to handle complex scenarios including inheritance hierarchies, foreign tables (via ermExtra), and various lock strengths and wait policies. Virtual relations like subqueries have a NULL relation pointer but still participate in the locking protocol for consistency.