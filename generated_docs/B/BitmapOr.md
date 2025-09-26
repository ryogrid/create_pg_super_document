# BitmapOr

## Location
[src/include/nodes/plannodes.h:370-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L370-L375)

## Overview
The BitmapOr node generates the union of tuple bitmaps from multiple sub-plans, implementing OR operations in bitmap index scans.

## Definition
```c
typedef struct BitmapOr
{
    Plan        plan;
    bool        isshared;           /* True if this node is shared across workers */
    List       *bitmapplans;        /* List of subplans that yield tuple bitmaps */
} BitmapOr;
```

## Detailed Description
The BitmapOr execution node performs bitmap union operations as part of PostgreSQL's bitmap index scan mechanism. It takes multiple child plans that each produce a tuple bitmap (typically BitmapIndexScan nodes) and combines them using bitwise OR operations to produce a final bitmap containing tuples that satisfy any of the conditions.

This node is essential for optimizing complex WHERE clauses with multiple indexed conditions connected by OR operators. Instead of performing multiple separate index scans and then merging results, the bitmap approach first creates bitmaps of candidate tuples from each index, unions them efficiently using bitwise OR, and then performs a single heap scan on the resulting tuples.

The node supports parallel execution through the isshared flag, allowing multiple worker processes to collaborate on the same bitmap operation. Like BitmapAnd, the targetlist and qual fields are unused since this node only manipulates bitmaps.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information (targetlist and qual are always NIL)
- `isshared`: Boolean flag indicating whether this node is shared across parallel workers
- `bitmapplans`: List of child Plan nodes that must be of types yielding tuple bitmaps (typically BitmapIndexScan nodes)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references from this struct)
- Called from (representative examples):
  - [ExecInitBitmapOr](../E/ExecInitBitmapOr.md)
  - [MultiExecBitmapOr](../M/MultiExecBitmapOr.md)
  - [make_bitmap_or](../m/make_bitmap_or.md)
  - [bitmap_subplan_mark_shared](../b/bitmap_subplan_mark_shared.md)

## Notes and Other Information
- Part of PostgreSQL's bitmap index scan optimization strategy
- Performs efficient union of multiple tuple bitmaps using bitwise OR
- Subplans must be of types that yield tuple bitmaps (not regular tuple streams)
- The targetlist and qual fields are unused and always NIL
- Commonly used with complex WHERE clauses involving OR conditions on multiple indexed columns
- Supports parallel execution through the isshared mechanism
- Results are typically consumed by BitmapHeapScan nodes
- More efficient than multiple separate index scans when OR conditions exist
- Located in src/include/nodes/plannodes.h:370-375