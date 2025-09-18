# ExecInitNode

## Location
src/backend/executor/execProcnode.c: 142 - 424

## Overview
ExecInitNode recursively initializes all nodes in a PostgreSQL execution plan tree, creating the corresponding execution state structures for each node type.

## Definition


## Detailed Description
ExecInitNode serves as the central dispatcher for initializing PostgreSQL's execution plan tree. It performs a recursive depth-first initialization of all plan nodes, converting each Plan node into its corresponding PlanState execution structure. The function uses a large switch statement to handle over 30 different node types, including control nodes (Result, Append), scan nodes (SeqScan, IndexScan), join nodes (NestLoop, HashJoin), and materialization nodes (Sort, Agg).

The function performs several critical tasks:
- Stack depth checking to prevent overflow during deep plan tree initialization
- Dispatching to type-specific initialization functions for each node type
- Setting up execution procedure nodes via ExecSetExecProcNode
- Initializing any initPlans (subplans) associated with the node
- Setting up instrumentation for performance monitoring when enabled

The initialization process ensures that all necessary data structures, memory contexts, and execution state are properly established before query execution begins.

## Parameters / Member Variables
- : The Plan node from the query planner to be initialized (can be NULL for leaf nodes)
- : The shared execution state structure for the entire plan tree
- : Bitwise OR of execution flags that control initialization behavior (defined in executor.h)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow prevention)
  - nodeTag (node type identification)
  - ExecInit* functions for each node type (ExecInitSeqScan, ExecInitAgg, etc.)
  - ExecSetExecProcNode (execution procedure setup)
  - ExecInitSubPlan (subplan initialization)
  - InstrAlloc (instrumentation setup)
- Called from (representative examples):
  - InitPlan (main executor initialization)
  - EvalPlanQualStart (EPQ initialization)
  - Various ExecInit* functions for recursive child node initialization

## Notes and Other Information
- Returns NULL when reaching leaf nodes (when node parameter is NULL)
- The function is recursive and can handle arbitrarily deep plan trees
- Each node type has its own specialized ExecInit function that handles type-specific initialization
- The function sets up both the execution state and the execution procedure for each node
- Instrumentation is conditionally initialized based on estate->es_instrument setting
- Stack depth is checked to prevent stack overflow during initialization of deep plan trees