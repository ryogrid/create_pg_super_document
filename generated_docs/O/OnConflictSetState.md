# OnConflictSetState

## Location
[src/include/nodes/execnodes.h:407-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L407-L415)

## Overview
OnConflictSetState holds the executor state for an ON CONFLICT DO UPDATE operation, managing tuple storage and projection during conflict resolution.

## Definition

```c
typedef struct OnConflictSetState
{
	NodeTag		type;

	TupleTableSlot *oc_Existing;	/* slot to store existing target tuple in */
	TupleTableSlot *oc_ProjSlot;	/* CONFLICT ... SET ... projection target */
	ProjectionInfo *oc_ProjInfo;	/* for ON CONFLICT DO UPDATE SET */
	ExprState  *oc_WhereClause; /* state for the WHERE clause */
} OnConflictSetState;
```
## Detailed Description
OnConflictSetState manages the execution state for PostgreSQL's ON CONFLICT DO UPDATE functionality (also known as UPSERT). When an INSERT statement encounters a conflict with existing data, this structure provides the necessary state to perform the UPDATE portion of the operation.

The structure maintains tuple slots for both the existing conflicting tuple and the projection target, along with the projection information needed to compute the updated values and any WHERE clause conditions that must be evaluated.

## Parameters / Member Variables
- `type`: NodeTag identifier for the structure type
- `*oc_Existing`: Tuple slot to store the existing target tuple that conflicts with the insert
- `*oc_ProjSlot`: Tuple slot for the CONFLICT ... SET ... projection target
- `*oc_ProjInfo`: ProjectionInfo structure containing projection instructions for ON CONFLICT DO UPDATE SET operations
- `*oc_WhereClause`: Expression state for evaluating the WHERE clause in ON CONFLICT DO UPDATE
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [TupleTableSlot](../T/TupleTableSlot.md)
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - [ExprState](../E/ExprState.md)
- Called from (representative examples):
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md)
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [ResultRelInfo](../R/ResultRelInfo.md)

## Notes and Other Information
- Essential component of PostgreSQL's UPSERT (INSERT ... ON CONFLICT DO UPDATE) functionality
- Provides efficient conflict resolution by maintaining separate slots for existing and projected tuples
- The WHERE clause support allows conditional updates during conflict resolution
- Works closely with ResultRelInfo to manage per-relation conflict handling
- Part of PostgreSQL's advanced INSERT capabilities for handling duplicate key scenarios