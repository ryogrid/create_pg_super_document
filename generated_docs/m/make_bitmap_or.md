# make_bitmap_or

## Location
src/backend/optimizer/plan/createplan.c: 5934 - 5948

## Overview
Creates a BitmapOr plan node that represents the logical OR operation between multiple bitmap index scans in PostgreSQL's query execution plan tree.

## Definition


## Detailed Description
The  function constructs a BitmapOr plan node, which is used in PostgreSQL's query planner to combine multiple bitmap index scans using a logical OR operation. This node type is essential for executing queries that can benefit from multiple indexes on the same table, where rows satisfying any of the index conditions should be included in the result set.

The function initializes a new BitmapOr node with standard plan node fields set to their default values (NIL for targetlist and qual, NULL for child trees) and assigns the provided list of bitmap plans to the bitmapplans field. The resulting node will coordinate the execution of multiple bitmap index scans and merge their results using OR logic.

## Parameters / Member Variables
- : A List containing the child bitmap plan nodes that will be combined using OR logic

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the BitmapOr node)
  - BitmapOr (plan node structure)
- Called from (representative examples):
  - create_bitmap_subplan (in createplan.c:3433)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's used internally by the plan creation subsystem
- The function follows PostgreSQL's standard pattern for creating plan nodes with minimal initialization
- BitmapOr nodes are typically created when the query planner determines that multiple indexes can be used with OR conditions
- The actual bitmap OR operation logic is handled during plan execution, not in this creation function