# ExecInitValuesScan

## Location
[src/backend/executor/nodeValuesscan.c:210-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeValuesscan.c#L210-L327)

## Overview
ExecInitValuesScan initializes a ValuesScanState node for executing VALUES clauses, setting up the necessary execution context, expression evaluation, and scan infrastructure.

## Definition
```c
ValuesScanState *ExecInitValuesScan(ValuesScan *node, EState *estate, int eflags)
```

## Detailed Description
ExecInitValuesScan performs comprehensive initialization of a VALUES scan node. It creates and configures a ValuesScanState structure, sets up dual expression contexts (one for per-row processing and one for scan operations), initializes the scan tuple slot based on the VALUES column types, and prepares expression evaluation infrastructure.

The function handles two types of expression processing: simple expressions that can be evaluated on-demand during execution, and complex expressions containing SubPlans that must be initialized upfront. For SubPlan-containing expressions, it disables JIT compilation to optimize performance since these expressions are typically used only once.

The function also establishes the scan infrastructure by calling standard executor initialization functions for result types, projection info, and scan qualification, ensuring that VALUES scans integrate properly with the rest of PostgreSQL's execution framework.

## Parameters / Member Variables
- `node`: ValuesScan plan node containing the VALUES lists and scan information
- `estate`: EState containing global execution state and context
- `eflags`: Execution flags controlling initialization behavior (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - outerPlan/innerPlan (assertions)
  - [ExecValuesScan](ExecValuesScan.md)
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecTypeFromExprList](ExecTypeFromExprList.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - [contain_subplans](../c/contain_subplans.md)
  - [ExecInitExprList](ExecInitExprList.md)
- Called from:
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Creates two expression contexts: rowcontext for per-VALUES-row processing and a standard context for scan operations
- Converts VALUES expression lists into arrays for efficient runtime access
- Handles SubPlan-containing expressions specially by pre-initializing them and disabling JIT compilation
- Initializes curr_idx to -1 to indicate no current row initially
- The function ensures VALUES scans have no child plans (outerPlan/innerPlan must be NULL)
- Uses virtual tuple slots for efficiency since VALUES generate synthetic tuples rather than reading from storage