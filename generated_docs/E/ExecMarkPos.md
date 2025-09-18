# ExecMarkPos

## Location
src/backend/executor/execAmi.c: 326 - 374

## Overview
ExecMarkPos marks the current scan position in a plan node, enabling later restoration to this position, primarily used by MergeJoin operations that require repositioning within sorted input streams.

## Definition


## Detailed Description
ExecMarkPos is a dispatcher function that marks the current position in a scan node so that execution can later return to this exact position using ExecRestrPos. This capability is essential for implementing certain join algorithms, particularly MergeJoin, which may need to re-read portions of the inner relation when processing duplicate values.

The function operates by examining the node type and calling the appropriate node-specific mark position function. Only certain node types support mark/restore functionality:

- **IndexScanState**: Can mark positions within index scans
- **IndexOnlyScanState**: Can mark positions within index-only scans  
- **CustomScanState**: Delegates to custom scan providers
- **MaterialState**: Can mark positions within materialized results
- **SortState**: Can mark positions within sorted data
- **ResultState**: Can mark positions within result nodes

The mark/restore capability is specifically designed for nodes that can produce sorted output, as MergeJoin requires sorted input streams. Node types that cannot produce sorted output typically don't need this functionality, and if mark/restore is required but not supported, the planner compensates by inserting a Material node.

## Parameters / Member Variables
- : Pointer to the PlanState node where the current position should be marked. The node must be of a type that supports mark/restore operations.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (node type identification)
  - ExecIndexMarkPos (index scan position marking)
  - ExecIndexOnlyMarkPos (index-only scan position marking)
  - ExecCustomMarkPos (custom scan position marking)
  - ExecMaterialMarkPos (material node position marking)
  - ExecSortMarkPos (sort node position marking)
  - ExecResultMarkPos (result node position marking)
- Called from (representative examples):
  - ExecMergeJoin (merge join operations)
  - ExecResultMarkPos (result node delegation)

## Notes and Other Information
- Unlike ExecReScan, this function does not throw a hard error for unsupported node types - it only logs a DEBUG2 message, allowing the caller to handle unsupported cases
- The comment explains that mark/restore is primarily needed for immediate inner children of MergeJoin nodes
- Node types that cannot produce sorted output don't typically need mark/restore capability
- When mark/restore is needed but not supported by a node type, the planner automatically inserts a Material node to provide this capability
- The mark position is stored internally within each node type's state structure and can be restored later with ExecRestrPos