# BitmapAndState

## Location
[src/include/nodes/execnodes.h:1527-1532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1527-L1532)

## Overview
BitmapAndState is the runtime state structure for the BitmapAnd executor node, which performs logical AND operations on bitmap indexes from multiple child plans.

## Definition


## Detailed Description
BitmapAndState manages the execution of bitmap AND operations in PostgreSQL's bitmap index scan optimization. It combines multiple bitmap indexes by performing logical AND operations, resulting in a bitmap that represents the intersection of all input bitmaps. This is used to efficiently filter rows when multiple index conditions need to be satisfied simultaneously.

## Parameters / Member Variables
-   PID TTY          TIME CMD
14209 ?        00:00:00 bash
14236 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node fields
- : Array of PlanState pointers for each input bitmap plan (typically BitmapIndexScan nodes)
- : Number of input plans in the bitmapplans array

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references beyond base types)
- Called from (representative examples):
  - [ExecBitmapAnd](../E/ExecBitmapAnd.md)
  - [ExecInitBitmapAnd](../E/ExecInitBitmapAnd.md)
  - [MultiExecBitmapAnd](../M/MultiExecBitmapAnd.md)
  - [ExecEndBitmapAnd](../E/ExecEndBitmapAnd.md)

## Notes and Other Information
- Used in bitmap heap scan optimization where multiple indexes can be combined for efficient row filtering
- The AND operation creates a bitmap containing only pages that satisfy all input conditions
- Typically used when a query has multiple WHERE conditions that can each use a different index
- Results in fewer page reads compared to scanning each index separately and filtering at the tuple level
- The structure is relatively simple compared to other executor nodes since the main work is delegating to child plans and combining their bitmap results