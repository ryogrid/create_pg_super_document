# BitmapAnd

## Location
src/include/nodes/plannodes.h: 356 - 360

## Overview
The BitmapAnd node generates the intersection of tuple bitmaps from multiple sub-plans, implementing AND operations in bitmap index scans.

## Definition
```c
typedef struct BitmapAnd
{
    Plan        plan;
    List       *bitmapplans;        /* List of subplans that yield tuple bitmaps */
} BitmapAnd;
```

## Detailed Description
The BitmapAnd execution node performs bitmap intersection operations as part of PostgreSQL's bitmap index scan mechanism. It takes multiple child plans that each produce a tuple bitmap (typically BitmapIndexScan nodes) and combines them using bitwise AND operations to produce a final bitmap containing only tuples that satisfy all conditions.

This node is crucial for optimizing complex WHERE clauses with multiple indexed conditions. Instead of performing multiple separate index scans and then filtering, the bitmap approach first creates bitmaps of candidate tuples from each index, intersects them efficiently, and then performs a single heap scan on the resulting tuples.

The targetlist and qual fields of the plan are unused and always set to NIL since this node only manipulates bitmaps and doesn't process actual tuples.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information (targetlist and qual are always NIL)
- `bitmapplans`: List of child Plan nodes that must be of types yielding tuple bitmaps (typically BitmapIndexScan nodes)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references from this struct)
- Called from (representative examples):
  - ExecInitBitmapAnd
  - make_bitmap_and
  - bitmap_subplan_mark_shared
  - set_plan_refs

## Notes and Other Information
- Part of PostgreSQL's bitmap index scan optimization strategy
- Performs efficient intersection of multiple tuple bitmaps using bitwise AND
- Subplans must be of types that yield tuple bitmaps (not regular tuple streams)
- The targetlist and qual fields are unused and always NIL
- Commonly used with complex WHERE clauses involving multiple indexed columns
- Results are typically consumed by BitmapHeapScan nodes
- More efficient than multiple separate index scans when multiple conditions exist
- Located in src/include/nodes/plannodes.h:356-360