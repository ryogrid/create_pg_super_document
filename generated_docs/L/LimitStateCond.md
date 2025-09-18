# LimitStateCond

## Location
src/include/nodes/execnodes.h: 2834 - 2835

## Overview
An enumeration that tracks the execution state of LIMIT/OFFSET operations, managing the complex state machine required for efficient tuple window processing with support for WITH TIES clause.

## Definition


## Detailed Description
LimitStateCond implements a state machine for PostgreSQL's LIMIT and OFFSET clause processing. The LIMIT node enforces row count restrictions by selecting the desired subrange from its subplan's output. The state machine handles complex scenarios including parameter recomputation during execution, empty result sets, proper window boundary management, and special handling for the WITH TIES option which requires comparing tuples for equality to return additional tied rows beyond the specified limit.

## Parameters / Member Variables
- : Starting state before offset/count parameters have been computed and evaluated
- : State entered after parameter recomputation, typically during rescan operations
- : Terminal state when there are no tuples that satisfy the offset/limit constraints
- : Normal operational state when returning tuples within the specified limit window
- : Special state for WITH TIES processing when returning tied rows beyond the limit
- : State when the subplan has reached EOF but still within the limit window
- : State when the limit count has been exceeded and processing should stop
- : State when processing is before the offset position in the result set

## Dependencies
- Functions called/Symbols referenced: (None - this is a simple enumeration)
- Called from (representative examples):
  - LimitState (used as lstate field at execnodes.h:2845)
  - nodeLimit.c:ExecLimit() (extensive state machine logic throughout function)
  - nodeLimit.c:ExecReScanLimit() (state reset to LIMIT_RESCAN at line 415)
  - nodeLimit.c:ExecInitLimit() (initialization to LIMIT_INITIAL at line 463)

## Notes and Other Information
This state machine is essential for correct LIMIT/OFFSET implementation, particularly handling edge cases like WITH TIES clause, parameter rescanning, and backward/forward tuple movement. The state transitions ensure that the exact number of specified rows are returned while supporting complex scenarios like tied values that exceed the limit count. The implementation optimizes performance by tracking position state rather than recounting tuples on each operation.