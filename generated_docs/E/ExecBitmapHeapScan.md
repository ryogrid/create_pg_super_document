# ExecBitmapHeapScan

## Location
src/backend/executor/nodeBitmapHeapscan.c: 581 - 594

## Overview
ExecBitmapHeapScan is the main execution function for bitmap heap scan nodes that retrieves the next qualifying tuple by delegating to the generic scan framework with bitmap-specific access and recheck methods.

## Definition
static TupleTableSlot *ExecBitmapHeapScan(PlanState *pstate)

## Detailed Description
ExecBitmapHeapScan serves as the primary execution entry point for bitmap heap scan operations in PostgreSQL's executor. This function implements the executor node interface for bitmap heap scans by leveraging the generic ExecScan framework. It casts the input PlanState to a BitmapHeapScanState and delegates the actual scanning work to ExecScan, providing bitmap-specific callback functions for tuple access (BitmapHeapNext) and tuple rechecking during EvalPlanQual processing (BitmapHeapRecheck).

The function represents the core of bitmap heap scan execution, coordinating between the bitmap results from index scans and the actual tuple retrieval from heap pages. It integrates seamlessly with PostgreSQL's execution framework while providing the specialized logic needed for bitmap-based tuple access patterns.

## Parameters / Member Variables
- : PlanState pointer that contains the execution state information, which gets cast to BitmapHeapScanState for bitmap heap scan specific operations

## Dependencies
- Functions called/Symbols referenced:
  - castNode (macro for type casting)
  - [ExecScan](ExecScan.md)
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (passed as ExecScanAccessMtd callback)
  - [BitmapHeapRecheck](../B/BitmapHeapRecheck.md) (passed as ExecScanRecheckMtd callback)
- Data types referenced:
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md)
  - [PlanState](../P/PlanState.md)
  - TupleTableSlot
- Called from:
  - [ExecInitBitmapHeapScan](ExecInitBitmapHeapScan.md) (src/backend/executor/nodeBitmapHeapscan.c:705)

## Notes and Other Information
- This is a static function, only accessible within the nodeBitmapHeapscan.c file
- The function follows PostgreSQL's executor node pattern by providing a unified interface while delegating to specialized implementations
- Uses the generic ExecScan framework which handles common scanning logic like projection, qualification, and EvalPlanQual processing
- The bitmap-specific behavior is provided through callback functions BitmapHeapNext and BitmapHeapRecheck
- Returns a TupleTableSlot containing the next qualifying tuple, or NULL when no more tuples are available
- Part of the executor node interface that gets called repeatedly to retrieve successive tuples from the scan