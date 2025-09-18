# make_bitmap_and

## Location
src/backend/optimizer/plan/createplan.c: 5919 - 5933

## Overview
Creates and initializes a BitmapAnd plan node that performs logical AND operations on bitmap index scans for efficient multi-index queries.

## Definition
```c
static BitmapAnd *
make_bitmap_and(List *bitmapplans)
```

## Detailed Description
This function constructs a BitmapAnd plan node, which is used to combine multiple bitmap index scans using a logical AND operation. This is a key optimization technique in PostgreSQL for queries that can benefit from multiple indexes on the same table. The BitmapAnd node takes the result bitmaps from multiple index scans and performs a bitwise AND operation to identify rows that satisfy all the combined conditions. This allows PostgreSQL to efficiently handle complex WHERE clauses with multiple indexed conditions.

## Parameters / Member Variables
- `bitmapplans`: List of child plan nodes that produce bitmaps (typically BitmapIndexScan nodes)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate BitmapAnd node)
  - BitmapAnd (struct type)
- Called from (representative examples):
  - [create_bitmap_subplan](../c/create_bitmap_subplan.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the createplan.c file
- Unlike other plan nodes, BitmapAnd has no targetlist or qual since it only manipulates bitmaps, not actual tuple data
- The lefttree and righttree are set to NULL because child plans are stored in the bitmapplans list instead
- BitmapAnd nodes are typically followed by BitmapHeapScan nodes that use the resulting bitmap to efficiently fetch the actual tuples
- This node type is crucial for PostgreSQL's bitmap index scan optimization, allowing efficient execution of queries with multiple WHERE conditions that can each use different indexes
- The resulting bitmap represents the intersection of all input bitmaps, containing only rows that satisfy all the combined index conditions