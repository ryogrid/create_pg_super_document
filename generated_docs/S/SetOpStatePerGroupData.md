# SetOpStatePerGroupData

## Location
[src/backend/executor/nodeSetOp.c:64-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L64-L68)

## Overview
SetOpStatePerGroupData is a per-group working state structure used in PostgreSQL's SetOp executor node to track duplicate counts for left and right inputs during set operation processing.

## Definition

```c
typedef struct SetOpStatePerGroupData
{
	long		numLeft;		/* number of left-input dups in group */
	long		numRight;		/* number of right-input dups in group */
}			SetOpStatePerGroupData;
```
## Detailed Description
SetOpStatePerGroupData represents the per-group working state that maintains counters for duplicate tuples from both left and right input sources during set operations (UNION, INTERSECT, EXCEPT). This structure is initialized at the start of processing an input tuple group and updated as each input tuple is processed.

The structure supports two different execution modes:
- **SETOP_SORTED mode**: Only one instance is needed and is kept in the plan state node
- **SETOP_HASHED mode**: The hash table contains one instance for each distinct tuple group

This working state is essential for determining the final output behavior of set operations, as it tracks how many duplicates exist from each input side, which is necessary to implement the correct semantics for operations like UNION ALL, INTERSECT, and EXCEPT.

## Parameters / Member Variables
- : Counter tracking the number of duplicate tuples from the left input that belong to the current group
- : Counter tracking the number of duplicate tuples from the right input that belong to the current group

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure definition)
- Called from (representative examples):
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md) (src/backend/executor/nodeSetOp.c:391)
  - [ExecInitSetOp](../E/ExecInitSetOp.md) (src/backend/executor/nodeSetOp.c:569)
  - [SetOpStatePerGroup](SetOpStatePerGroup.md) (src/include/nodes/execnodes.h:2779)

## Notes and Other Information
- This structure is defined in src/backend/executor/nodeSetOp.c:64-68
- The structure is designed to be lightweight, containing only two long integers to minimize memory overhead
- The choice between SETOP_SORTED and SETOP_HASHED modes affects how instances of this structure are allocated and managed
- The counters are used to implement the correct duplicate handling semantics required by different SQL set operations