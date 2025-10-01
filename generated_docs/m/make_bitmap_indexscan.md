# make_bitmap_indexscan

## Location
[src/backend/optimizer/plan/createplan.c:5605-5625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5605-L5625)

## Overview
Creates and initializes a BitmapIndexScan plan node that generates a bitmap of heap page locations by scanning an index, typically used as input to a subsequent BitmapHeapScan operation.

## Definition

```c
static BitmapIndexScan *
make_bitmap_indexscan(Index scanrelid,
					  Oid indexid,
					  List *indexqual,
					  List *indexqualorig)
```
## Detailed Description
This function constructs a BitmapIndexScan plan node, which is the first stage of PostgreSQL's bitmap scan execution strategy. Unlike regular index scans that retrieve tuples directly, a bitmap index scan creates a bitmap indicating which heap pages contain qualifying tuples. This bitmap is then used by a BitmapHeapScan node to efficiently read only the relevant pages from the heap table. The approach is particularly effective when the selectivity suggests that tuples are scattered across many pages, as it minimizes random I/O by reading each page only once.

## Parameters / Member Variables
- : Range table index of the relation whose index is being scanned
- : Object identifier of the specific index to be scanned for bitmap generation
- : Processed index qualification conditions that will be applied during index scanning
- : Original (unprocessed) form of the index qualification conditions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the BitmapIndexScan node)
- Called from (representative examples):
  - [create_bitmap_subplan](../c/create_bitmap_subplan.md)

## Notes and Other Information
- This is a static function within createplan.c for internal plan construction
- The target list and qual fields are explicitly set to NIL as they are not used in bitmap index scans
- Bitmap scans are most efficient when combined with multiple index conditions or when dealing with moderately selective queries
- The distinction between indexqual and indexqualorig allows the planner to maintain both processed and original forms of conditions
- Always used in conjunction with BitmapHeapScan for the complete bitmap scan operation

## Simplified Source

```c
static BitmapIndexScan *
make_bitmap_indexscan(Index scanrelid, Oid indexid, List *indexqual, List *indexqualorig)
{
    BitmapIndexScan *node = makeNode(BitmapIndexScan);
    Plan *plan = &node->scan.plan;

    // No targetlist or qual needed - this node only produces bitmaps
    plan->targetlist = NIL;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;

    // Set scan parameters
    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;

    return node;
}
```