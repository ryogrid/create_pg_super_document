# ExecReScanIndexOnlyScan

## Location
[src/backend/executor/nodeIndexonlyscan.c:363-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L363-L397)

## Overview
Recalculates runtime-dependent scan keys and restarts an index-only scan from the beginning, integrating key evaluation with scan restart for optimal performance.

## Definition

```c
void
ExecReScanIndexOnlyScan(IndexOnlyScanState *node)
```
## Detailed Description
ExecReScanIndexOnlyScan implements the rescan operation for index-only scan nodes, handling the complete restart of a scan operation. The function's primary responsibilities include recalculating runtime scan keys and reinitializing the underlying index scan.

Runtime keys are scan key values that cannot be determined at planning time, typically involving parameters, subquery references, or volatile expressions. When runtime keys are present, the function resets the expression context to prevent memory leaks, then recalculates all runtime key values using the current execution context.

After ensuring that all runtime keys are current, the function restarts the underlying index scan using index_rescan, passing the updated scan keys and order-by keys. Finally, it calls the generic scan framework's rescan function to complete the restart process.

This design integrates runtime key evaluation directly into the rescan process, which was historically handled separately, providing better uniformity across different scan types and improved efficiency.

## Parameters / Member Variables
- `*node`: IndexOnlyScanState containing all scan state information, including runtime keys, scan descriptors, and context information
## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext: Clears expression context to prevent memory leaks during key recalculation
  - [ExecIndexEvalRuntimeKeys](ExecIndexEvalRuntimeKeys.md): Evaluates runtime-dependent scan key expressions
  - [index_rescan](../i/index_rescan.md): Restarts the underlying index scan with updated keys
  - [ExecScanReScan](ExecScanReScan.md): Handles generic scan framework rescan operations
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md): Generic executor rescan dispatcher that calls this function for index-only scan nodes

## Notes and Other Information
- Memory management is carefully handled by resetting the expression context before recalculating runtime keys
- The function assumes that all runtime keys need recalculation on each rescan call
- Integration of key evaluation and scan restart improves consistency with other scan node types
- Runtime keys are marked as ready after evaluation to avoid redundant recalculation
- The function handles both parameterized and non-parameterized rescans efficiently
- Part of PostgreSQL's executor node interface, called by the generic executor framework

## Simplified Source

```c
void ExecReScanIndexOnlyScan(IndexOnlyScanState *node) {
    // Step 1: Recalculate runtime scan keys if present
    if (node->ioss_NumRuntimeKeys != 0) {
        ExprContext *econtext = node->ioss_RuntimeContext;

        // Reset context to prevent memory leaks
        ResetExprContext(econtext);

        // Evaluate all runtime key expressions
        ExecIndexEvalRuntimeKeys(econtext,
                                node->ioss_RuntimeKeys,
                                node->ioss_NumRuntimeKeys);
    }
    node->ioss_RuntimeKeysReady = true;

    // Step 2: Restart the index scan with updated keys
    if (node->ioss_ScanDesc) {
        index_rescan(node->ioss_ScanDesc,
                    node->ioss_ScanKeys, node->ioss_NumScanKeys,
                    node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);
    }

    // Step 3: Complete the scan framework rescan
    ExecScanReScan(&node->ss);
}
```