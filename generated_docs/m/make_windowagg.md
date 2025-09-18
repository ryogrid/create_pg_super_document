# make_windowagg

## Location
[src/backend/optimizer/plan/createplan.c:6628-6669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6628-L6669)

## Overview
Creates and initializes a WindowAgg plan node for executing window function operations in PostgreSQL query plans.

## Definition
```c
static WindowAgg *make_windowagg(List *tlist, Index winref,
                                 int partNumCols, AttrNumber *partColIdx, Oid *partOperators, Oid *partCollations,
                                 int ordNumCols, AttrNumber *ordColIdx, Oid *ordOperators, Oid *ordCollations,
                                 int frameOptions, Node *startOffset, Node *endOffset,
                                 Oid startInRangeFunc, Oid endInRangeFunc,
                                 Oid inRangeColl, bool inRangeAsc, bool inRangeNullsFirst,
                                 List *runCondition, List *qual, bool topWindow, Plan *lefttree)
```

## Detailed Description
This static function constructs a WindowAgg plan node that handles window function execution in PostgreSQL. Window functions operate over a "window" of rows related to the current row, defined by PARTITION BY and ORDER BY clauses with optional frame specifications. The function initializes all necessary components for window processing including partitioning columns, ordering columns, frame boundaries, and range functions for RANGE frame types. It also handles run conditions for optimization and maintains both original and processed versions of conditions for EXPLAIN output.

## Parameters / Member Variables
- `tlist`: Target list defining the output columns of the window operation
- `winref`: Index reference to the window specification in the query
- `partNumCols`: Number of partitioning columns for PARTITION BY clause
- `partColIdx`: Array of attribute numbers for partitioning columns  
- `partOperators`: Array of operator OIDs for partitioning column comparisons
- `partCollations`: Array of collation OIDs for partitioning columns
- `ordNumCols`: Number of ordering columns for ORDER BY clause
- `ordColIdx`: Array of attribute numbers for ordering columns
- `ordOperators`: Array of operator OIDs for ordering column comparisons
- `ordCollations`: Array of collation OIDs for ordering columns
- `frameOptions`: Bit flags specifying frame type and bounds (ROWS/RANGE/GROUPS)
- `startOffset`: Expression for frame start boundary offset
- `endOffset`: Expression for frame end boundary offset
- `startInRangeFunc`: Function OID for start boundary in RANGE frames
- `endInRangeFunc`: Function OID for end boundary in RANGE frames
- `inRangeColl`: Collation for range boundary calculations
- `inRangeAsc`: Whether range ordering is ascending
- `inRangeNullsFirst`: Whether NULLs come first in range ordering
- `runCondition`: Conditions that can skip window computation for optimization
- `qual`: Additional qualification conditions to apply
- `topWindow`: Whether this is the top-level window in a window function stack
- `lefttree`: Left child plan node providing input tuples

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create WindowAgg node)
- Types referenced:
  - WindowAgg (the window aggregation plan node structure)
- Called from (representative examples):
  - [create_windowagg_plan](../c/create_windowagg_plan.md)

## Notes and Other Information
- This is a static function, only accessible within the createplan.c file
- Maintains both `runCondition` and `runConditionOrig` fields, with the latter being a duplicate for EXPLAIN output
- The right child plan node is always set to NULL as window operations are unary
- Window functions can be stacked, with `topWindow` indicating the outermost window operation
- Frame options use bit flags to encode different frame types (ROWS, RANGE, GROUPS) and boundary specifications
- Range frames require special functions (`startInRangeFunc`, `endInRangeFunc`) to compute frame boundaries based on value ranges rather than row counts