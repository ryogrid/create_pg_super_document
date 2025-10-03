# make_bitmap_or

## Location
[src/backend/optimizer/plan/createplan.c:5934-5948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5934-L5948)

## Overview
Creates a BitmapOr plan node that represents the logical OR operation between multiple bitmap index scans in PostgreSQL's query execution plan tree.

## Definition

```c
static BitmapOr *
make_bitmap_or(List *bitmapplans)
```
## Detailed Description
The  function constructs a BitmapOr plan node, which is used in PostgreSQL's query planner to combine multiple bitmap index scans using a logical OR operation. This node type is essential for executing queries that can benefit from multiple indexes on the same table, where rows satisfying any of the index conditions should be included in the result set.

The function initializes a new BitmapOr node with standard plan node fields set to their default values (NIL for targetlist and qual, NULL for child trees) and assigns the provided list of bitmap plans to the bitmapplans field. The resulting node will coordinate the execution of multiple bitmap index scans and merge their results using OR logic.

## Parameters / Member Variables
- `*bitmapplans`: A List containing the child bitmap plan nodes that will be combined using OR logic
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the BitmapOr node)
  - [BitmapOr](../B/BitmapOr.md) (plan node structure)
- Called from (representative examples):
  - [create_bitmap_subplan](../c/create_bitmap_subplan.md) (in createplan.c:3433)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's used internally by the plan creation subsystem
- The function follows PostgreSQL's standard pattern for creating plan nodes with minimal initialization
- [BitmapOr](../B/BitmapOr.md) nodes are typically created when the query planner determines that multiple indexes can be used with OR conditions
- The actual bitmap OR operation logic is handled during plan execution, not in this creation function

## Simplified Source

```c
static BitmapOr *
make_bitmap_or(List *bitmapplans)
{
    BitmapOr *node = makeNode(BitmapOr);
    Plan *plan = &node->plan;

    // Initialize plan fields - no targetlist or quals needed for bitmap operations
    plan->targetlist = NIL;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;

    // Store child bitmap plans for OR operation
    node->bitmapplans = bitmapplans;

    return node;
}
```