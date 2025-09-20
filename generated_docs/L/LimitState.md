# LimitState

## Location
[src/include/nodes/execnodes.h:2836-2851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2836-L2851)

## Overview
LimitState is the execution state structure for Limit nodes in PostgreSQL's executor, implementing LIMIT and OFFSET clauses in SQL queries, including support for WITH TIES option.

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
LimitState manages the execution state for Limit nodes, which implement SQL LIMIT and OFFSET functionality. The structure supports dynamic limit and offset values through expression evaluation, tracks the current position in the result set, and implements a state machine to handle various scenarios including EOF conditions and WITH TIES semantics. The state machine ensures correct behavior during rescans and handles edge cases like empty result sets and window boundaries.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common executor node information
- `limitOffset`: Expression state for the OFFSET parameter (NULL if no OFFSET)
- `limitCount`: Expression state for the COUNT parameter (NULL if no LIMIT)
- `limitOption`: Enumeration specifying the type of limit (WITH TIES, etc.)
- `offset`: Current evaluated OFFSET value
- `count`: Current evaluated COUNT value
- `noCount`: Boolean flag to ignore the count when true
- `lstate`: State machine condition tracking current execution phase
- `position`: 1-based index of the last tuple returned to track progress
- `subSlot`: Tuple slot containing the last tuple obtained from the subplan
- `eqfunction`: Expression state for tuple equality comparison (used with WITH TIES)
- `last_slot`: Tuple slot used for evaluating ties in WITH TIES mode

## Dependencies
- Functions called/Symbols referenced:
  - [LimitOption](LimitOption.md)
  - [LimitStateCond](LimitStateCond.md)
- Called from (representative examples):
  - [ExecLimit](../E/ExecLimit.md)
  - [ExecInitLimit](../E/ExecInitLimit.md)
  - [ExecEndLimit](../E/ExecEndLimit.md)
  - [recompute_limits](../r/recompute_limits.md)
  - [compute_tuples_needed](../c/compute_tuples_needed.md)

## Notes and Other Information
- Implements a state machine with conditions like LIMIT_INITIAL, LIMIT_INWINDOW, LIMIT_WINDOWEND_TIES
- Supports dynamic LIMIT and OFFSET values that can change during execution
- WITH TIES functionality requires tuple equality comparison to handle tied values correctly
- Handles complex scenarios like rescanning and EOF conditions properly
- Critical for implementing SQL standard LIMIT/OFFSET semantics with PostgreSQL extensions