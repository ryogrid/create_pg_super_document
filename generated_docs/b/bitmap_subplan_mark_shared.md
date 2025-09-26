# bitmap_subplan_mark_shared

## Location
[src/backend/optimizer/plan/createplan.c:5479-5508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5479-L5508)

## Overview
A recursive utility function that marks bitmap index scan subplans as shared, enabling them to be created in shared memory for parallel execution.

## Definition
```c
static void bitmap_subplan_mark_shared(Plan *plan)
```

## Detailed Description
This function recursively traverses bitmap scan plan trees and sets the 'isshared' flag on appropriate plan nodes to indicate that they should be created in shared memory. This is essential for parallel bitmap heap scans where multiple worker processes need to share the same bitmap data structure. The function handles three types of bitmap plan nodes: BitmapAnd (which it recursively processes on the first child), BitmapOr (which it marks as shared and recursively processes the first child), and BitmapIndexScan (which it directly marks as shared). The recursive approach ensures that complex bitmap scan trees with multiple levels of AND/OR operations are properly configured for parallel execution.

## Parameters / Member Variables
- `plan`: The bitmap plan node to be marked as shared (must be a BitmapAnd, BitmapOr, or BitmapIndexScan node)

## Dependencies
- Functions called/Symbols referenced:
  - [BitmapAnd](../B/BitmapAnd.md) (plan node type for bitmap AND operations)
  - [BitmapOr](../B/BitmapOr.md) (plan node type for bitmap OR operations)  
  - [BitmapIndexScan](../B/BitmapIndexScan.md) (plan node type for bitmap index scans)
  - nodeTag (to get the node type for error reporting)
  - linitial (to get the first element from plan lists)
  - [bitmap_subplan_mark_shared](bitmap_subplan_mark_shared.md) (recursive self-calls)
- Called from (representative examples):
  - [create_bitmap_scan_plan](../c/create_bitmap_scan_plan.md)
  - [bitmap_subplan_mark_shared](bitmap_subplan_mark_shared.md) (recursive calls)

## Notes and Other Information
- This is a static function within createplan.c for internal module use
- The function uses recursive descent to handle arbitrarily complex bitmap scan trees
- Only processes the first child of BitmapAnd and BitmapOr nodes, as subsequent children don't need shared marking in the current implementation
- Includes error handling for unexpected node types to ensure robust operation
- Critical for enabling parallel bitmap heap scans where shared bitmaps are required
- The isshared flag affects memory allocation strategy during plan execution