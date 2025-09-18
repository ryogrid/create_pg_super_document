# ExecCustomScan

## Location
src/backend/executor/nodeCustom.c: 114 - 124

## Overview
The execution function for Custom Scan nodes that delegates tuple retrieval to the custom scan provider's implementation.

## Definition
```c
static TupleTableSlot *ExecCustomScan(PlanState *pstate)
```

## Detailed Description
ExecCustomScan is a thin wrapper function that serves as the main execution entry point for custom scan nodes. It performs basic sanity checks and then delegates the actual tuple retrieval to the custom scan provider's ExecCustomScan method. This design allows custom scan providers to implement their own tuple scanning logic while integrating seamlessly with PostgreSQL's executor framework.

## Parameters / Member Variables
- `pstate`: The plan state node, which is cast to CustomScanState to access the custom scan methods

## Dependencies
- Functions called/Symbols referenced:
  - castNode (to cast PlanState to CustomScanState)
  - CHECK_FOR_INTERRUPTS (interrupt handling macro)
  - ExecCustomScan (via node->methods callback)
- Called from (representative examples):
  - ExecInitCustomScan (assigned as ExecProcNode)
  - PostgreSQL executor framework

## Notes and Other Information
- This is a static function that serves as the standard executor interface for custom scans
- The actual scanning logic is implemented by the custom scan provider in their ExecCustomScan callback
- Includes interrupt checking for query cancellation and other signal handling
- The function assumes the custom scan provider has properly implemented the ExecCustomScan method