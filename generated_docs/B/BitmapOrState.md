# BitmapOrState

## Location
[src/include/nodes/execnodes.h:1538-1543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1538-L1543)

## Overview
BitmapOrState is the runtime state structure for the BitmapOr executor node, which performs logical OR operations on bitmap indexes from multiple child plans.

## Definition

```c
typedef struct BitmapOrState
{
	PlanState	ps;				/* its first field is NodeTag */
	PlanState **bitmapplans;	/* array of PlanStates for my inputs */
	int			nplans;			/* number of input plans */
} BitmapOrState;
```
## Detailed Description
BitmapOrState manages the execution of bitmap OR operations in PostgreSQL's bitmap index scan optimization. It combines multiple bitmap indexes by performing logical OR operations, resulting in a bitmap that represents the union of all input bitmaps. This is used to efficiently identify rows when any of multiple index conditions need to be satisfied.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common executor node fields
- `bitmapplans`: Array of PlanState pointers for each input bitmap plan (typically BitmapIndexScan nodes)
- `nplans`: Number of input plans in the bitmapplans array

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references beyond base types)
- Called from (representative examples):
  - [ExecBitmapOr](../E/ExecBitmapOr.md)
  - [ExecInitBitmapOr](../E/ExecInitBitmapOr.md)
  - [MultiExecBitmapOr](../M/MultiExecBitmapOr.md)
  - [ExecEndBitmapOr](../E/ExecEndBitmapOr.md)

## Notes and Other Information
- Used in bitmap heap scan optimization where multiple indexes can be combined for efficient row filtering
- The OR operation creates a bitmap containing pages that satisfy any of the input conditions
- Typically used when a query has multiple WHERE conditions connected by OR that can each use a different index
- More efficient than executing separate index scans and merging results at the tuple level
- Structure is identical to BitmapAndState but semantics differ (union vs intersection of bitmaps)
- Can significantly reduce I/O when multiple conditions in an OR clause can leverage different indexes