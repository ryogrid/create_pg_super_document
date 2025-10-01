# ExecInitNode

## Location
[src/backend/executor/execProcnode.c:142-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execProcnode.c#L142-L424)

## Overview
ExecInitNode recursively initializes all nodes in a PostgreSQL execution plan tree, creating the corresponding execution state structures for each node type.

## Definition

```c
PlanState *
ExecInitNode(Plan *node, EState *estate, int eflags)
```
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
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow prevention)
  - nodeTag (node type identification)
  - ExecInit* functions for each node type (ExecInitSeqScan, ExecInitAgg, etc.)
  - [ExecSetExecProcNode](ExecSetExecProcNode.md) (execution procedure setup)
  - [ExecInitSubPlan](ExecInitSubPlan.md) (subplan initialization)
  - [InstrAlloc](../I/InstrAlloc.md) (instrumentation setup)
- Called from (representative examples):
  - [InitPlan](../I/InitPlan.md) (main executor initialization)
  - [EvalPlanQualStart](EvalPlanQualStart.md) (EPQ initialization)
  - Various ExecInit* functions for recursive child node initialization

## Notes and Other Information
- Returns NULL when reaching leaf nodes (when node parameter is NULL)
- The function is recursive and can handle arbitrarily deep plan trees
- Each node type has its own specialized ExecInit function that handles type-specific initialization
- The function sets up both the execution state and the execution procedure for each node
- [Instrumentation](../I/Instrumentation.md) is conditionally initialized based on estate->es_instrument setting
- Stack depth is checked to prevent stack overflow during initialization of deep plan trees

## Simplified Source

```c
PlanState *ExecInitNode(Plan *node, EState *estate, int eflags) {
    PlanState *result;
    List *subps;
    ListCell *l;

    // Handle leaf nodes
    if (node == NULL)
        return NULL;

    // Prevent stack overflow during deep plan tree initialization
    check_stack_depth();

    // Dispatch to appropriate initialization function based on node type
    switch (nodeTag(node)) {
        // Control nodes
        case T_Result:
            result = (PlanState *) ExecInitResult((Result *) node, estate, eflags);
            break;
        case T_ModifyTable:
            result = (PlanState *) ExecInitModifyTable((ModifyTable *) node, estate, eflags);
            break;
        case T_Append:
            result = (PlanState *) ExecInitAppend((Append *) node, estate, eflags);
            break;

        // Scan nodes
        case T_SeqScan:
            result = (PlanState *) ExecInitSeqScan((SeqScan *) node, estate, eflags);
            break;
        case T_IndexScan:
            result = (PlanState *) ExecInitIndexScan((IndexScan *) node, estate, eflags);
            break;
        case T_BitmapHeapScan:
            result = (PlanState *) ExecInitBitmapHeapScan((BitmapHeapScan *) node, estate, eflags);
            break;

        // Join nodes
        case T_NestLoop:
            result = (PlanState *) ExecInitNestLoop((NestLoop *) node, estate, eflags);
            break;
        case T_MergeJoin:
            result = (PlanState *) ExecInitMergeJoin((MergeJoin *) node, estate, eflags);
            break;
        case T_HashJoin:
            result = (PlanState *) ExecInitHashJoin((HashJoin *) node, estate, eflags);
            break;

        // Materialization nodes
        case T_Sort:
            result = (PlanState *) ExecInitSort((Sort *) node, estate, eflags);
            break;
        case T_Agg:
            result = (PlanState *) ExecInitAgg((Agg *) node, estate, eflags);
            break;
        case T_Group:
            result = (PlanState *) ExecInitGroup((Group *) node, estate, eflags);
            break;

        // ... (other node types handled similarly)

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            result = NULL;
            break;
    }

    // Set up the execution procedure for this node
    ExecSetExecProcNode(result, result->ExecProcNode);

    // Initialize any subplans associated with this node
    subps = NIL;
    foreach(l, node->initPlan) {
        SubPlan *subplan = (SubPlan *) lfirst(l);
        SubPlanState *sstate;

        sstate = ExecInitSubPlan(subplan, result);
        subps = lappend(subps, sstate);
    }
    result->initPlan = subps;

    // Set up performance instrumentation if enabled
    if (estate->es_instrument)
        result->instrument = InstrAlloc(1, estate->es_instrument, result->async_capable);

    return result;
}
```