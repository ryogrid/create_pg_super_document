# UniqueState

## Location
[src/include/nodes/execnodes.h:2656-2660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2656-L2660)

## Overview
UniqueState is a structure that represents the execution state for PostgreSQL's UNIQUE node, which is used to eliminate duplicate tuples from sorted input by comparing consecutive tuples.

## Definition

```c
typedef struct UniqueState
{
	PlanState	ps;				/* its first field is NodeTag */
	ExprState  *eqfunction;		/* tuple equality qual */
} UniqueState;
```
## Detailed Description
UniqueState maintains the execution state for Unique nodes, which are positioned "on top of" sort nodes to discard duplicate tuples returned from the sort phase. The node operates by comparing the current tuple from the subplan with the previously fetched tuple (stored in its result slot). If the two tuples are identical in all interesting fields, the node fetches another tuple from the sort and continues the comparison process until a unique tuple is found or no more tuples are available.

## Parameters / Member Variables
- `ps`: PlanState structure containing common executor node state information, with NodeTag as its first field
- `eqfunction`: ExprState pointer containing the tuple equality qualification used to determine if two tuples are identical in the relevant fields

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited structure)
  - [ExprState](../E/ExprState.md) (for equality function)
- Called from (representative examples):
  - [ExecUnique](../E/ExecUnique.md) (main execution function)
  - [ExecInitUnique](../E/ExecInitUnique.md) (initialization function)
  - [ExecEndUnique](../E/ExecEndUnique.md) (cleanup function)
  - [ExecReScanUnique](../E/ExecReScanUnique.md) (rescan function)

## Notes and Other Information
- [Unique](Unique.md) nodes are typically used in conjunction with sort nodes to implement SQL DISTINCT operations
- The equality function is crucial for determining which fields are considered "interesting" for uniqueness comparison
- The node maintains state across multiple tuple fetches to enable efficient duplicate detection
- Located in src/include/nodes/execnodes.h:2656-2660