# NestLoopState

## Location
[src/include/nodes/execnodes.h:2103-2109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2103-L2109)

## Overview
NestLoopState represents the execution state for nested loop join operations, maintaining state information needed to perform nested loop joins between outer and inner relations.

## Definition

```c
typedef struct NestLoopState
{
	JoinState	js;				/* its first field is NodeTag */
	bool		nl_NeedNewOuter;
	bool		nl_MatchedOuter;
	TupleTableSlot *nl_NullInnerTupleSlot;
} NestLoopState;
```
## Detailed Description
NestLoopState extends JoinState to provide specific state management for nested loop join execution. This structure tracks the progress of the nested loop algorithm, which iterates through each tuple in the outer relation and, for each outer tuple, scans the inner relation to find matching tuples. The state information helps coordinate this two-level iteration and handles special cases like outer joins where null values must be generated for unmatched outer tuples.

## Parameters / Member Variables
- `js`: Base JoinState structure containing common join execution state
- `nl_NeedNewOuter`: Boolean flag indicating whether a new outer tuple is needed on the next call to the execution function
- `nl_MatchedOuter`: Boolean flag tracking whether a join match has been found for the current outer tuple (important for outer joins)
- `*nl_NullInnerTupleSlot`: Pre-prepared tuple slot containing null values for all inner relation attributes, used for left outer joins when no matching inner tuple is found
## Dependencies
- Functions called/Symbols referenced:
  - [JoinState](../J/JoinState.md) (inherited base structure)
  - [TupleTableSlot](../T/TupleTableSlot.md) (for null inner tuple storage)
- Called from (representative examples):
  - [ExecNestLoop](../E/ExecNestLoop.md) (main execution function)
  - [ExecInitNestLoop](../E/ExecInitNestLoop.md) (initialization function)
  - [ExecEndNestLoop](../E/ExecEndNestLoop.md) (cleanup function)
  - [ExecReScanNestLoop](../E/ExecReScanNestLoop.md) (rescan function)

## Notes and Other Information
- This structure is specifically designed for nested loop join execution, which has O(N*M) complexity where N and M are the sizes of outer and inner relations respectively
- The nl_NullInnerTupleSlot is pre-allocated during initialization to avoid repeated allocation during execution for left outer joins
- The state flags (nl_NeedNewOuter, nl_MatchedOuter) coordinate the dual-level iteration pattern inherent in nested loop joins
- Nested loop joins are often used when no suitable join indexes exist or when one of the relations is very small
- The structure is defined in src/include/nodes/execnodes.h at lines 2103-2109