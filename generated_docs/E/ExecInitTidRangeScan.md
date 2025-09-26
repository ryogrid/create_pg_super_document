# ExecInitTidRangeScan

## Location
[src/backend/executor/nodeTidrangescan.c:347-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L347-L405)

## Overview
ExecInitTidRangeScan initializes a TID range scan node by setting up the execution state, opening the scan relation, and preparing all necessary components for TID range scanning operations.

## Definition
```c
TidRangeScanState *ExecInitTidRangeScan(TidRangeScan *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitTidRangeScan is responsible for the complete initialization of a TID range scan operation. The function creates and configures a TidRangeScanState structure that will be used throughout the scan's execution. It performs several key initialization steps: creating the state structure, setting up the expression context, opening the scan relation, initializing tuple slots and result types, setting up projection information, initializing qualifier expressions, and creating the TID expression list.

The function follows PostgreSQL's standard executor initialization pattern, ensuring that all necessary components are properly configured before the scan begins execution. It notably sets up the ExecProcNode pointer to point to ExecTidRangeScan, establishing the execution chain for this node type.

## Parameters / Member Variables
- `node`: A TidRangeScan pointer containing the plan node produced by the planner with scan configuration and parameters
- `estate`: An EState pointer representing the execution state initialized in InitPlan, providing global execution context
- `eflags`: An integer containing execution flags that control various aspects of the scan initialization

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new TidRangeScanState structure)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (creates expression context for the node)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md) (opens the relation to be scanned)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md) (initializes the scan tuple slot)
  - RelationGetDescr (gets relation descriptor)
  - [table_slot_callbacks](../t/table_slot_callbacks.md) (gets table slot callbacks)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (initializes result type and target list)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md) (sets up projection information)
  - [ExecInitQual](ExecInitQual.md) (initializes qualifier expressions)
  - [TidExprListCreate](../T/TidExprListCreate.md) (creates and processes TID expression list)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (generic plan node initialization dispatcher)

## Notes and Other Information
- The function sets `trss_inScan` to false, indicating the scan is not yet in progress
- The scan descriptor is initially set to NULL since no table scan is established at initialization
- TID range scans don't use traditional table scan descriptors but manage their own scanning mechanism
- The function ensures all expressions and projection information are properly initialized before returning
- The returned TidRangeScanState structure is fully configured and ready for execution