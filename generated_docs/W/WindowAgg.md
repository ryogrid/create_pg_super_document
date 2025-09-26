# WindowAgg

## Location
src/include/nodes/plannodes.h: 1038 - 1106

## Overview
WindowAgg is a specialized plan node that implements window functions in PostgreSQL, processing data within sliding windows defined by PARTITION BY and ORDER BY clauses with optional frame specifications.

## Definition


## Detailed Description
The WindowAgg node implements SQL window functions such as ROW_NUMBER(), RANK(), SUM() OVER(), and LAG()/LEAD(). It processes input data partitioned by specified columns and ordered within each partition. The node maintains a sliding window frame that can be defined using ROWS or RANGE clauses with PRECEDING/FOLLOWING boundaries. It supports both bounded and unbounded frames, and can efficiently handle peer groups (rows with identical values in ORDER BY columns). The node can run multiple window functions simultaneously if they share the same window specification, optimizing execution by avoiding redundant sorting and partitioning operations.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Window reference ID used to identify the window specification
- : Number of columns in the PARTITION BY clause
- : Array of attribute numbers for partition columns
- : Array of equality operators for partition column comparisons
- : Array of collations for partition columns
- : Number of columns in the ORDER BY clause within the window
- : Array of attribute numbers for ordering columns
- : Array of comparison operators for ordering columns
- : Array of collations for ordering columns
- : Bit flags defining frame type (ROWS/RANGE/GROUPS) and bounds
- : Expression defining the starting boundary of the frame
- : Expression defining the ending boundary of the frame
- : Optimized conditions for early termination
- : Original run condition for EXPLAIN output
- : Function for RANGE frame start boundary calculations
- : Function for RANGE frame end boundary calculations
- : Collation for in-range function calls
- : Sort order for range comparisons
- : NULL handling for range comparisons
- : True if this is the topmost WindowAgg node in the plan

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)
  - Index
  - AttrNumber
  - Oid
  - Node
  - List

- Called from (representative examples):
  - ExecInitWindowAgg (executor/nodeWindowAgg.c:2374)
  - create_windowagg_plan (optimizer/plan/createplan.c:2619)
  - make_windowagg (optimizer/plan/createplan.c:6636)
  - begin_partition (executor/nodeWindowAgg.c:1083)
  - update_frameheadpos (executor/nodeWindowAgg.c:1487)
  - WinRowsArePeers (executor/nodeWindowAgg.c:3256)

## Notes and Other Information
- WindowAgg nodes require their input to be sorted by partition columns first, then by ordering columns within each partition
- The node can handle complex frame specifications including UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, and numeric/interval offsets
- Multiple window functions with identical window specifications can be computed by a single WindowAgg node for efficiency
- The topWindow field helps optimize execution when multiple WindowAgg nodes are stacked
- RANGE frames with PRECEDING/FOLLOWING require special in_range functions for proper boundary calculations
- The node supports runtime optimization through runCondition to skip unnecessary computation
- Frame boundaries can be dynamically computed using expressions, not just constant offsets
- Peer detection is crucial for functions like RANK() and DENSE_RANK() that treat equal values specially