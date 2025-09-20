# JoinState

## Location
[src/include/nodes/execnodes.h:2086-2093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2086-L2093)

## Overview
JoinState is a superclass for state nodes of join plans in PostgreSQL's executor, providing common state information and functionality shared by all join operation types.

## Definition

```c
typedef struct JoinState
{
	PlanState	ps;
	JoinType	jointype;
	bool		single_match;	/* True if we should skip to next outer tuple
								 * after finding one inner match */
	ExprState  *joinqual;		/* JOIN quals (in addition to ps.qual) */
} JoinState;
```
## Detailed Description
JoinState serves as the base structure for all join execution state nodes in PostgreSQL's executor. It inherits from PlanState and adds join-specific state information. This structure provides the common foundation for nested loop joins, merge joins, hash joins, and other join algorithms. The structure maintains the join type, optimization flags, and join qualification expressions that are evaluated during join processing.

## Parameters / Member Variables

- `ps`: Base PlanState structure containing common execution state information
- `jointype`: The type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `single_match`: Boolean flag indicating whether to skip to the next outer tuple after finding one inner match (optimization for certain join types)
- `joinqual`: Pointer to ExprState containing JOIN qualification expressions that are evaluated in addition to the base plan's qualification expressions

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited base structure)
  - JoinType (enum for join types)
  - ExprState (for join qualification expressions)
- Called from (representative examples):
  - [NestLoopState](../N/NestLoopState.md) (inherits from JoinState)
  - [MergeJoinState](../M/MergeJoinState.md) (inherits from JoinState)
  - [HashJoinState](../H/HashJoinState.md) (inherits from JoinState)

## Notes and Other Information
- This is an abstract base structure - actual join execution uses specific subclasses like NestLoopState, MergeJoinState, or HashJoinState
- The single_match optimization is particularly useful for semi-joins and anti-joins where only the existence of a match matters
- Join qualifications in joinqual are evaluated separately from the base plan qualifications in ps.qual
- The structure is defined in src/include/nodes/execnodes.h at lines 2086-2093