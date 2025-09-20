# ExecInitCustomScan

## Location
[src/backend/executor/nodeCustom.c:26-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCustom.c#L26-L113)

## Overview
Initializes the state for a Custom Scan node during executor startup, setting up the scan relation, tuple descriptors, and calling the custom scan provider's initialization callback.

## Definition

```c
CustomScanState *
ExecInitCustomScan(CustomScan *cscan, EState *estate, int eflags)
```
## Detailed Description
ExecInitCustomScan is the initialization function for custom scan nodes in PostgreSQL's executor. It performs the standard initialization tasks required for any scan node and then delegates to the custom scan provider for specialized initialization. The function allocates a CustomScanState object through the provider's CreateCustomScanState callback, sets up the scan relation if needed, initializes tuple slots with appropriate slot operations, and establishes result projection. The custom scan provider has full control over the CustomScanState allocation and can embed it in a larger structure for provider-specific state.

## Parameters / Member Variables
- `cscan`: The CustomScan plan node containing the scan configuration and provider methods
- `estate`: The execution state containing transaction and memory context information
- `eflags`: Execution flags controlling initialization behavior (e.g., EXEC_FLAG_BACKWARD)

## Dependencies
- Functions called/Symbols referenced:
  - CreateCustomScanState (via cscan->methods)
  - [ExecCustomScan](ExecCustomScan.md)
  - ExecAssignExprContext
  - ExecOpenScanRelation
  - [ExecTypeFromTL](ExecTypeFromTL.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - [ExecAssignScanProjectionInfoWithVarno](ExecAssignScanProjectionInfoWithVarno.md)
  - [ExecInitQual](ExecInitQual.md)
  - BeginCustomScan (via css->methods)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- The custom scan provider must allocate the CustomScanState object and can embed it in a larger structure
- Supports both relation-based scans (scanrelid > 0) and custom tuple sources (custom_scan_tlist)
- Uses custom slot operations if provided by the scan provider, otherwise defaults to virtual slots
- The provider's BeginCustomScan callback is responsible for final initialization specific to the custom scan implementation