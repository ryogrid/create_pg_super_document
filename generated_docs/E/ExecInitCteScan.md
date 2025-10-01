# ExecInitCteScan

## Location
[src/backend/executor/nodeCtescan.c:175-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCtescan.c#L175-L287)

## Overview
ExecInitCteScan initializes a CteScanState node for executing CTE (Common Table Expression) scans, setting up shared tuplestore management and coordinating between multiple CTE scan instances.

## Definition

```c
CteScanState *
ExecInitCteScan(CteScan *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitCteScan performs comprehensive initialization of a CTE scan node, handling the complex coordination required when multiple CTE scan nodes may reference the same CTE query. The function implements a leader-follower pattern where:

1. **Leader Election**: The first CTE scan node to initialize becomes the "leader" and creates the shared tuplestore that will cache CTE query results
2. **Follower Setup**: Subsequent CTE scan nodes become "followers" and create their own read pointers into the shared tuplestore
3. **State Management**: Each node gets its own CteScanState but shares the underlying tuplestore and CTE query execution state

Key initialization steps include:
- Creating the CteScanState structure and linking it to the plan node and estate
- Setting up the shared tuplestore (for leader) or allocating a read pointer (for followers)  
- Initializing scan tuple slots based on the CTE query's result type
- Setting up expression contexts, projection info, and qualification expressions
- Configuring the execution flags, forcing REWIND capability for flexibility

The function uses a parameter execution slot to coordinate between multiple CTE scan instances, ensuring they all share the same underlying data and execution state.

## Parameters / Member Variables
- : CteScan plan node containing the CTE scan specification and parameters
- : EState executor state containing execution context and parameter values
- : Execution flags controlling scan behavior, with EXEC_FLAG_REWIND forced on

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Create new CteScanState node
  - [list_nth](../l/list_nth.md): Retrieve CTE plan state from estate's subplan list
  - castNode: Safely cast parameter value to CteScanState
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md): Create new tuplestore for leader
  - [tuplestore_set_eflags](../t/tuplestore_set_eflags.md): Configure tuplestore execution flags
  - [tuplestore_alloc_read_pointer](../t/tuplestore_alloc_read_pointer.md): Allocate read pointer for follower
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md): Select active read pointer
  - [tuplestore_rescan](../t/tuplestore_rescan.md): Reset read pointer to beginning
  - [ExecAssignExprContext](ExecAssignExprContext.md): Set up expression evaluation context
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md): Initialize scan tuple slot
  - [ExecGetResultType](ExecGetResultType.md): Get result tuple descriptor from CTE plan
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md): Initialize result type from target list
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md): Set up projection information
  - [ExecInitQual](ExecInitQual.md): Initialize qualification expressions
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md): Called during plan tree initialization

## Notes and Other Information
- Forces EXEC_FLAG_REWIND to allow rescanning, even if not requested by upper levels (marked as XXX FIXME for potential optimization)
- Asserts that CTE scans have no child plans (outer or inner)
- Uses ParamExecData mechanism to coordinate leader election between multiple CTE scan instances
- Leader creates shared tuplestore with work_mem memory limit
- Followers get independent read pointers but share the same tuplestore data
- The cteplanstate points to the already-initialized CTE query plan from the estate's subplan list
- Located at src/backend/executor/nodeCtescan.c:175-287

## Simplified Source

```c
CteScanState *
ExecInitCteScan(CteScan *node, EState *estate, int eflags)
{
    // Force REWIND capability for CTE rescanning
    eflags |= EXEC_FLAG_REWIND;

    // Create and initialize the CTE scan state
    CteScanState *scanstate = makeNode(CteScanState);
    scanstate->ss.ps.plan = (Plan *) node;
    scanstate->ss.ps.state = estate;
    scanstate->ss.ps.ExecProcNode = ExecCteScan;
    scanstate->eflags = eflags;
    scanstate->cte_table = NULL;
    scanstate->eof_cte = false;

    // Find the CTE query plan state
    scanstate->cteplanstate = (PlanState *) list_nth(estate->es_subplanstates,
                                                     node->ctePlanId - 1);

    // Handle leader/follower coordination for shared tuplestore
    ParamExecData *prmdata = &(estate->es_param_exec_vals[node->cteParam]);
    scanstate->leader = castNode(CteScanState, DatumGetPointer(prmdata->value));

    if (scanstate->leader == NULL)
    {
        // I am the leader - create shared tuplestore
        prmdata->value = PointerGetDatum(scanstate);
        scanstate->leader = scanstate;
        scanstate->cte_table = tuplestore_begin_heap(true, false, work_mem);
        tuplestore_set_eflags(scanstate->cte_table, scanstate->eflags);
        scanstate->readptr = 0;
    }
    else
    {
        // I am a follower - create my own read pointer
        scanstate->readptr = tuplestore_alloc_read_pointer(scanstate->leader->cte_table,
                                                          scanstate->eflags);
        tuplestore_select_read_pointer(scanstate->leader->cte_table, scanstate->readptr);
        tuplestore_rescan(scanstate->leader->cte_table);
    }

    // Set up expression context and tuple handling
    ExecAssignExprContext(estate, &scanstate->ss.ps);
    ExecInitScanTupleSlot(estate, &scanstate->ss,
                          ExecGetResultType(scanstate->cteplanstate),
                          &TTSOpsMinimalTuple);

    // Initialize result type and projection
    ExecInitResultTypeTL(&scanstate->ss.ps);
    ExecAssignScanProjectionInfo(&scanstate->ss);

    // Initialize qualification expressions
    scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

    return scanstate;
}
```