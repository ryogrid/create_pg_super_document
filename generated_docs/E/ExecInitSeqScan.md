# ExecInitSeqScan

## Location
[src/backend/executor/nodeSeqscan.c:123-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L123-L183)

## Overview
ExecInitSeqScan initializes a sequential scan node by creating the execution state, setting up the scan relation, and initializing all necessary components for tuple processing and projection.

## Definition
```c
SeqScanState *ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitSeqScan performs comprehensive initialization of a sequential scan execution node. It creates a SeqScanState structure, validates that the node has no outer or inner plans (as sequential scans are leaf nodes), sets up the execution context, opens the scan relation, initializes the tuple slot with appropriate row type and callbacks, sets up result type and projection information, and initializes any qualification expressions. The function follows PostgreSQL's standard node initialization pattern and ensures all components are properly configured before scan execution begins.

## Parameters / Member Variables
- `node`: SeqScan pointer containing the plan node information including scan relation ID and qualification conditions
- `estate`: EState pointer containing the execution state with snapshot, direction, and other execution context
- `eflags`: Integer flags controlling execution behavior and optimization settings

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (SeqScanState)
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - outerPlan
  - innerPlan
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)
  - NODESEQSCAN_H

## Notes and Other Information
- Returns a fully initialized SeqScanState ready for execution
- Includes assertions to ensure no outer or inner plans exist (sequential scans are leaf operations)
- Sets the ExecProcNode function pointer to ExecSeqScan for execution dispatch
- Uses RelationGetDescr to get the tuple descriptor for slot initialization
- Handles both result tuple projection and qualification expression setup
- Part of PostgreSQL's node initialization infrastructure

## Simplified Source

```c
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
    SeqScanState *scanstate;

    // Validate no child plans (sequential scans are leaf nodes)
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    // Create and initialize state structure
    scanstate = makeNode(SeqScanState);
    scanstate->ss.ps.plan = (Plan *) node;
    scanstate->ss.ps.state = estate;
    scanstate->ss.ps.ExecProcNode = ExecSeqScan;

    // Create expression context
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    // Open the scan relation
    scanstate->ss.ss_currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

    // Initialize scan tuple slot with appropriate row type
    ExecInitScanTupleSlot(estate, &scanstate->ss,
                         RelationGetDescr(scanstate->ss.ss_currentRelation),
                         table_slot_callbacks(scanstate->ss.ss_currentRelation));

    // Initialize result handling
    ExecInitResultTypeTL(&scanstate->ss.ps);
    ExecAssignScanProjectionInfo(&scanstate->ss);

    // Initialize qualification expressions
    scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

    return scanstate;
}
```