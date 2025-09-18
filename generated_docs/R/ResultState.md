# ResultState

## Location
src/include/nodes/execnodes.h: 1320 - 1326

## Overview
ResultState is an execution state structure for Result nodes in PostgreSQL's executor, which represent plans that return constant tuples or apply unconditional filters.

## Definition


## Detailed Description
ResultState maintains the execution state for Result nodes, which are used in PostgreSQL's executor to handle plans that either produce constant result tuples or apply qualification conditions. Result nodes are typically used for queries that don't require table access, such as SELECT with constant expressions, or for applying WHERE clauses that can be evaluated independently of table data. The structure tracks whether execution is complete and manages any constant qualification expressions that need to be evaluated.

## Parameters / Member Variables
-   PID TTY          TIME CMD
13664 ?        00:00:00 bash
13691 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common execution state fields like the node tag and plan information
- : Expression state for any constant qualification conditions that need to be evaluated
- : Boolean flag indicating whether the Result node has finished producing tuples
- : Boolean flag indicating whether qualification expressions need to be checked during execution

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited base structure)
  - ExprState (for qualification expression state)
- Called from (representative examples):
  - [ExecResult](../E/ExecResult.md)
  - [ExecInitResult](../E/ExecInitResult.md)
  - [ExecEndResult](../E/ExecEndResult.md)
  - [ExecReScanResult](../E/ExecReScanResult.md)
  - [ExecResultMarkPos](../E/ExecResultMarkPos.md)
  - [ExecResultRestrPos](../E/ExecResultRestrPos.md)

## Notes and Other Information
Result nodes are fundamental components in PostgreSQL's execution engine, often used for simple queries that don't require complex table operations. The rs_done flag is crucial for ensuring that Result nodes produce the correct number of tuples, while rs_checkqual optimizes execution by determining whether qualification checking is necessary. This state structure is primarily manipulated by functions in nodeResult.c and is part of the broader PlanState hierarchy used throughout PostgreSQL's executor.