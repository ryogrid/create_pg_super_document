# UniquePathMethod

## Location
[src/include/nodes/pathnodes.h:2025-2026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2025-L2026)

## Overview
UniquePathMethod is an enumeration that specifies the algorithm to be used for eliminating duplicate rows in a UniquePath operation within PostgreSQL's query optimizer.

## Definition

```c
typedef struct UniquePath
{
	Path		path;
	Path	   *subpath;
	UniquePathMethod umethod;
	List	   *in_operators;	/* equality operators of the IN clause */
	List	   *uniq_exprs;		/* expressions to be made unique */
} UniquePath;
```
## Detailed Description
UniquePathMethod defines the three possible strategies for implementing row uniqueness elimination in PostgreSQL's query execution plans. This enumeration is used by the UniquePath node to determine how to remove duplicate rows from the result set.

The choice between methods depends on various factors including the size of the input data, whether the input is already sorted, memory availability, and cost estimates. The optimizer analyzes these factors to select the most efficient method for each specific query context.

UNIQUE_PATH_NOOP represents an optimization where the input is already guaranteed to be unique (e.g., due to primary key constraints or previous operations), eliminating the need for any duplicate removal processing.

## Parameters / Member Variables
- : Input data is already unique, no duplicate elimination needed
- : Use hash-based deduplication (typically faster for unsorted data)
- : Use sort-based deduplication (efficient when data is already partially sorted)

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum, so it doesn't reference other symbols directly)

- Called from (representative examples):
  - [create_unique_plan](../c/create_unique_plan.md) (in createplan.c for plan creation)
  - [create_unique_path](../c/create_unique_path.md) (in pathnode.c for path generation)
  - UniquePath struct (as umethod field)

## Notes and Other Information
- Used exclusively within UniquePath structures to control deduplication strategy
- The choice between HASH and SORT methods is made by the optimizer based on cost estimates
- NOOP method is an important optimization that avoids unnecessary work when uniqueness is already guaranteed
- Hash-based method generally preferred for large datasets with good hash distribution
- Sort-based method leveraged when input has existing ordering or when memory is constrained
- Critical for implementing SQL DISTINCT and UNION operations efficiently