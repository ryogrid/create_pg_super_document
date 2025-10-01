# ExecInitTidScan

## Location
[src/backend/executor/nodeTidscan.c:488-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidscan.c#L488-L548)

## Overview
ExecInitTidScan initializes a TID scan node's execution state, creates scan keys, and opens the base relation for TID-based scanning operations.

## Definition
```c
TidScanState *
ExecInitTidScan(TidScan *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitTidScan performs comprehensive initialization of a TID scan node for execution. The function creates and configures a TidScanState structure, sets up the execution context, opens the target relation for scanning, and initializes all necessary scan components. It establishes the scan tuple slot with the appropriate tuple descriptor and table slot callbacks, initializes result type and projection information, sets up qualification expressions, and creates the TID expression list that will be used during scan execution. The function follows the standard PostgreSQL executor initialization pattern, ensuring the scan node is fully prepared for execution.

## Parameters / Member Variables
- `node`: TidScan plan node produced by the planner, containing scan configuration and target information
- `estate`: EState execution state initialized in InitPlan, providing the execution environment context
- `eflags`: Execution flags that control various aspects of node initialization and execution behavior

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create TidScanState structure)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (to create expression context)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md) (to open the target relation)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md) (to initialize scan tuple slot)
  - RelationGetDescr (to get relation tuple descriptor)
  - [table_slot_callbacks](../t/table_slot_callbacks.md) (to get table-specific slot callbacks)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (to initialize result tuple descriptor)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md) (to set up scan projection)
  - [ExecInitQual](ExecInitQual.md) (to initialize qualification expressions)
  - [TidExprListCreate](../T/TidExprListCreate.md) (to create TID expression list)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (generic executor node initialization dispatch function)
  - NODETIDSCAN_H (header file declaration)

## Notes and Other Information
- Initializes tss_TidPtr to -1 to indicate no current scan position
- Sets ss_currentScanDesc to NULL since TID scans don't use heap scan descriptors
- The function establishes both scan and projection capabilities for the node
- TID expression list creation is crucial for evaluating TID qualification conditions
- Follows the standard PostgreSQL executor initialization pattern with proper resource setup
- Returns a fully initialized TidScanState ready for execution by ExecTidScan

## Simplified Source

```c
TidScanState *
ExecInitTidScan(TidScan *node, EState *estate, int eflags)
{
    TidScanState *tidstate;
    Relation currentRelation;

    // Create and initialize state structure
    tidstate = makeNode(TidScanState);
    tidstate->ss.ps.plan = (Plan *) node;
    tidstate->ss.ps.state = estate;
    tidstate->ss.ps.ExecProcNode = ExecTidScan;

    // Set up expression context
    ExecAssignExprContext(estate, &tidstate->ss.ps);

    // Initialize TID list state - not computed yet
    tidstate->tss_TidList = NULL;
    tidstate->tss_NumTids = 0;
    tidstate->tss_TidPtr = -1;

    // Open the relation to be scanned
    currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    tidstate->ss.ss_currentRelation = currentRelation;
    tidstate->ss.ss_currentScanDesc = NULL; // No heap scan descriptor needed

    // Initialize scan tuple slot with relation descriptor
    ExecInitScanTupleSlot(estate, &tidstate->ss,
                         RelationGetDescr(currentRelation),
                         table_slot_callbacks(currentRelation));

    // Initialize result type and projection
    ExecInitResultTypeTL(&tidstate->ss.ps);
    ExecAssignScanProjectionInfo(&tidstate->ss);

    // Initialize qualification expressions
    tidstate->ss.ps.qual =
        ExecInitQual(node->scan.plan.qual, (PlanState *) tidstate);

    // Create TID expression list for evaluation
    TidExprListCreate(tidstate);

    return tidstate;
}
```