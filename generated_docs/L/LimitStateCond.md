# LimitStateCond

## Location
[src/include/nodes/execnodes.h:2834-2835](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2834-L2835)

## Overview
An enumeration that tracks the execution state of LIMIT/OFFSET operations, managing the complex state machine required for efficient tuple window processing with support for WITH TIES clause.

## Definition

```c
typedef struct LimitState
{
	PlanState	ps;				/* its first field is NodeTag */
	ExprState  *limitOffset;	/* OFFSET parameter, or NULL if none */
	ExprState  *limitCount;		/* COUNT parameter, or NULL if none */
	LimitOption limitOption;	/* limit specification type */
	int64		offset;			/* current OFFSET value */
	int64		count;			/* current COUNT, if any */
	bool		noCount;		/* if true, ignore count */
	LimitStateCond lstate;		/* state machine status, as above */
	int64		position;		/* 1-based index of last tuple returned */
	TupleTableSlot *subSlot;	/* tuple last obtained from subplan */
	ExprState  *eqfunction;		/* tuple equality qual in case of WITH TIES
								 * option */
	TupleTableSlot *last_slot;	/* slot for evaluation of ties */
} LimitState;
```
## Detailed Description
LimitStateCond implements a state machine for PostgreSQL's LIMIT and OFFSET clause processing. The LIMIT node enforces row count restrictions by selecting the desired subrange from its subplan's output. The state machine handles complex scenarios including parameter recomputation during execution, empty result sets, proper window boundary management, and special handling for the WITH TIES option which requires comparing tuples for equality to return additional tied rows beyond the specified limit.

## Parameters / Member Variables
- : Starting state before offset/count parameters have been computed and evaluated
- : State entered after parameter recomputation, typically during rescan operations
- : Terminal state when there are no tuples that satisfy the offset/limit constraints
- : Normal operational state when returning tuples within the specified limit window
- : Special state for WITH TIES processing when returning tied rows beyond the limit
- : State when the subplan has reached EOF but still within the limit window
- : State when the limit count has been exceeded and processing should stop
- : State when processing is before the offset position in the result set

## Dependencies
- Functions called/Symbols referenced: (None - this is a simple enumeration)
- Called from (representative examples):
  - [LimitState](LimitState.md) (used as lstate field at execnodes.h:2845)
  - nodeLimit.c:ExecLimit() (extensive state machine logic throughout function)
  - nodeLimit.c:ExecReScanLimit() (state reset to LIMIT_RESCAN at line 415)
  - nodeLimit.c:ExecInitLimit() (initialization to LIMIT_INITIAL at line 463)

## Notes and Other Information
This state machine is essential for correct LIMIT/OFFSET implementation, particularly handling edge cases like WITH TIES clause, parameter rescanning, and backward/forward tuple movement. The state transitions ensure that the exact number of specified rows are returned while supporting complex scenarios like tied values that exceed the limit count. The implementation optimizes performance by tracking position state rather than recounting tuples on each operation.