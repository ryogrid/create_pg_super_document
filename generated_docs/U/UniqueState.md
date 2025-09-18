# UniqueState

## Location
src/include/nodes/execnodes.h: 2656 - 2660

## Overview
UniqueState is a structure that represents the execution state for PostgreSQL's UNIQUE node, which is used to eliminate duplicate tuples from sorted input by comparing consecutive tuples.

## Definition


## Detailed Description
UniqueState maintains the execution state for Unique nodes, which are positioned "on top of" sort nodes to discard duplicate tuples returned from the sort phase. The node operates by comparing the current tuple from the subplan with the previously fetched tuple (stored in its result slot). If the two tuples are identical in all interesting fields, the node fetches another tuple from the sort and continues the comparison process until a unique tuple is found or no more tuples are available.

## Parameters / Member Variables
-   PID TTY          TIME CMD
 8105 ?        00:00:00 bash
 8135 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: PlanState structure containing common executor node state information, with NodeTag as its first field
- : ExprState pointer containing the tuple equality qualification used to determine if two tuples are identical in the relevant fields

## Dependencies
- Functions called/Symbols referenced:
  - PlanState (inherited structure)
  - ExprState (for equality function)
- Called from (representative examples):
  - ExecUnique (main execution function)
  - ExecInitUnique (initialization function)
  - ExecEndUnique (cleanup function)
  - ExecReScanUnique (rescan function)

## Notes and Other Information
- Unique nodes are typically used in conjunction with sort nodes to implement SQL DISTINCT operations
- The equality function is crucial for determining which fields are considered "interesting" for uniqueness comparison
- The node maintains state across multiple tuple fetches to enable efficient duplicate detection
- Located in src/include/nodes/execnodes.h:2656-2660