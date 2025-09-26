# ExecInitFunctionScan

## Location
[src/backend/executor/nodeFunctionscan.c:279-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L279-L529)

## Overview
ExecInitFunctionScan initializes a FunctionScanState node for executing function scans, setting up tuple descriptors, expression contexts, and per-function state structures.

## Definition

```c
FunctionScanState *
ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitFunctionScan performs comprehensive initialization for function scan operations:

1. **State Structure Setup**: Creates and configures a FunctionScanState with proper executor integration, including setting the ExecProcNode to ExecFunctionScan.

2. **Optimization Detection**: Determines if this is a "simple" scan (single function, no ordinality) for performance optimization.

3. **Per-Function Initialization**: For each function in the scan:
   - Sets up table function result expressions via ExecInitTableFunctionResult
   - Builds appropriate tuple descriptors based on function return types
   - Handles composite types, scalar types, and explicit column definitions
   - Creates individual function slots when needed

4. **Tuple Descriptor Construction**: Creates the combined scan tuple descriptor by:
   - Copying from the single function's descriptor in simple cases
   - Building a composite descriptor from all functions in complex cases
   - Adding an ordinality column if requested

5. **Slot and Context Setup**: Initializes scan tuple slots, result projection, and creates a dedicated memory context for function argument evaluation.

The function handles various complexity levels from simple single-function scans to complex multi-function scans with ordinality columns.

## Parameters / Member Variables
- : FunctionScan plan node containing the function list and configuration
- : EState providing the executor state and memory contexts
- : Execution flags controlling scan behavior (EXEC_FLAG_BACKWARD supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecInitTableFunctionResult](ExecInitTableFunctionResult.md)
  - [BuildDescFromLists](../B/BuildDescFromLists.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - [get_expr_result_type](../g/get_expr_result_type.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md)
  - [TupleDescCopyEntry](../T/TupleDescCopyEntry.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - AllocSetContextCreate
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Supports both simple (single function) and complex (multiple functions/ordinality) scan modes
- Handles composite, scalar, and record function return types
- Creates dedicated argument evaluation context to avoid memory leaks
- Validates that function scans have no child nodes
- Properly initializes ordinality tracking even when not outputting ordinality columns
- Integrates with PostgreSQL's standard executor initialization patterns