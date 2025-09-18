# ExecReScanIndexOnlyScan

## Location
src/backend/executor/nodeIndexonlyscan.c: 363 - 397

## Overview
Recalculates runtime-dependent scan keys and restarts an index-only scan from the beginning, integrating key evaluation with scan restart for optimal performance.

## Definition


## Detailed Description
ExecReScanIndexOnlyScan implements the rescan operation for index-only scan nodes, handling the complete restart of a scan operation. The function's primary responsibilities include recalculating runtime scan keys and reinitializing the underlying index scan.

Runtime keys are scan key values that cannot be determined at planning time, typically involving parameters, subquery references, or volatile expressions. When runtime keys are present, the function resets the expression context to prevent memory leaks, then recalculates all runtime key values using the current execution context.

After ensuring that all runtime keys are current, the function restarts the underlying index scan using index_rescan, passing the updated scan keys and order-by keys. Finally, it calls the generic scan framework's rescan function to complete the restart process.

This design integrates runtime key evaluation directly into the rescan process, which was historically handled separately, providing better uniformity across different scan types and improved efficiency.

## Parameters / Member Variables
- : IndexOnlyScanState containing all scan state information, including runtime keys, scan descriptors, and context information

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext: Clears expression context to prevent memory leaks during key recalculation
  - ExecIndexEvalRuntimeKeys: Evaluates runtime-dependent scan key expressions
  - index_rescan: Restarts the underlying index scan with updated keys
  - ExecScanReScan: Handles generic scan framework rescan operations
- Called from (representative examples):
  - ExecReScan: Generic executor rescan dispatcher that calls this function for index-only scan nodes

## Notes and Other Information
- Memory management is carefully handled by resetting the expression context before recalculating runtime keys
- The function assumes that all runtime keys need recalculation on each rescan call
- Integration of key evaluation and scan restart improves consistency with other scan node types
- Runtime keys are marked as ready after evaluation to avoid redundant recalculation
- The function handles both parameterized and non-parameterized rescans efficiently
- Part of PostgreSQL's executor node interface, called by the generic executor framework