# ExecProcNode

## Location
src/include/executor/executor.h: 270 - 333

## Overview
ExecProcNode is the central function in PostgreSQL's query executor that processes a single plan node and returns the next tuple from that node, implementing the iterator model of query execution.

## Definition


## Detailed Description
ExecProcNode serves as the primary interface for tuple generation in PostgreSQL's executor. It implements the iterator pattern where each call returns the next available tuple from the specified plan node. The function first checks if any parameters have changed (via chgParam) and triggers a rescan if necessary, then delegates the actual tuple production to the node-specific execution function stored in the PlanState's ExecProcNode function pointer.

This design allows for a uniform interface across all plan node types while enabling each node type to implement its own specialized execution logic. The function is critical to PostgreSQL's volcano-style execution model, where data flows through the query plan tree one tuple at a time.

## Parameters / Member Variables
- : PlanState containing the plan node to execute and its associated state information

## Dependencies
- Functions called/Symbols referenced:
  - ExecReScan (triggered when parameters change)
  - node->ExecProcNode (node-specific execution function)
- Called from (representative examples):
  - ExecutePlan (main execution loop)
  - EvalPlanQualNext (during EPQ processing)
  - ExecProcNodeFirst (first-time node execution)
  - Various node-specific execution functions (for child nodes)
  - Dozens of executor nodes for retrieving tuples from child plans

## Notes and Other Information
- This is a static inline function defined in executor.h, making it efficiently accessible across the entire executor subsystem
- The chgParam mechanism allows for parameter-driven rescans, essential for nested loop joins and parameterized plans
- Each PlanState maintains its own ExecProcNode function pointer, set during node initialization to point to the appropriate node-type-specific execution function
- The function returns TupleTableSlot* which may be NULL when no more tuples are available
- This function is at the heart of PostgreSQL's pull-based execution model, where upper nodes request tuples from lower nodes as needed
- The uniform interface enables complex query plans to be executed recursively without upper nodes needing to know the specific types of their children