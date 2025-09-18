# LimitState

## Location
src/include/nodes/execnodes.h: 2836 - 2851

## Overview
LimitState is the execution state structure for Limit nodes in PostgreSQL's executor, implementing LIMIT and OFFSET clauses in SQL queries, including support for WITH TIES option.

## Definition


## Detailed Description
LimitState manages the execution state for Limit nodes, which implement SQL LIMIT and OFFSET functionality. The structure supports dynamic limit and offset values through expression evaluation, tracks the current position in the result set, and implements a state machine to handle various scenarios including EOF conditions and WITH TIES semantics. The state machine ensures correct behavior during rescans and handles edge cases like empty result sets and window boundaries.

## Parameters / Member Variables
-   PID TTY          TIME CMD
17516 ?        00:00:00 bash
17543 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node information
- : Expression state for the OFFSET parameter (NULL if no OFFSET)
- : Expression state for the COUNT parameter (NULL if no LIMIT)
- : Enumeration specifying the type of limit (WITH TIES, etc.)
- : Current evaluated OFFSET value
- : Current evaluated COUNT value
- : Boolean flag to ignore the count when true
- : State machine condition tracking current execution phase
- : 1-based index of the last tuple returned to track progress
- : Tuple slot containing the last tuple obtained from the subplan
- : Expression state for tuple equality comparison (used with WITH TIES)
- : Tuple slot used for evaluating ties in WITH TIES mode

## Dependencies
- Functions called/Symbols referenced:
  - [LimitOption](LimitOption.md)
  - [LimitStateCond](LimitStateCond.md)
- Called from (representative examples):
  - [ExecLimit](../E/ExecLimit.md)
  - [ExecInitLimit](../E/ExecInitLimit.md)
  - [ExecEndLimit](../E/ExecEndLimit.md)
  - [recompute_limits](../r/recompute_limits.md)
  - [compute_tuples_needed](../c/compute_tuples_needed.md)

## Notes and Other Information
- Implements a state machine with conditions like LIMIT_INITIAL, LIMIT_INWINDOW, LIMIT_WINDOWEND_TIES
- Supports dynamic LIMIT and OFFSET values that can change during execution
- WITH TIES functionality requires tuple equality comparison to handle tied values correctly
- Handles complex scenarios like rescanning and EOF conditions properly
- Critical for implementing SQL standard LIMIT/OFFSET semantics with PostgreSQL extensions