# ExecEndBitmapIndexScan

## Location
src/backend/executor/nodeBitmapIndexscan.c: 175 - 201

## Overview
ExecEndBitmapIndexScan performs cleanup operations for a bitmap index scan node, properly closing the index scan and releasing the index relation.

## Definition
```c
void ExecEndBitmapIndexScan(BitmapIndexScanState *node)
```

## Detailed Description
This function is responsible for the orderly shutdown of a bitmap index scan node. It performs the necessary cleanup operations to ensure that all resources associated with the bitmap index scan are properly released. The function first extracts the index relation descriptor and index scan descriptor from the node state, then systematically closes the index scan using the index access method's amendscan function, and finally closes the index relation itself without attempting to release any locks (since lock management is handled at a higher level).

The function follows the standard cleanup pattern used throughout the PostgreSQL executor, ensuring that resources are released in the reverse order of their acquisition to maintain proper resource management.

## Parameters / Member Variables
- `node`: Pointer to BitmapIndexScanState containing the execution state with index relation and scan descriptors to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - index_endscan (to properly terminate the index scan)
  - index_close (to close the index relation without lock release)
- Called from (representative examples):
  - ExecEndNode (from the general executor cleanup framework)

## Notes and Other Information
- Part of the standard executor node lifecycle (Init -> Exec -> End)
- Safely handles cases where descriptors might be NULL (no-op if not opened)
- Uses NoLock parameter with index_close since lock management is handled at higher levels
- Essential for preventing resource leaks in long-running queries
- Follows the PostgreSQL convention of cleaning up in reverse order of initialization
- Does not attempt to free memory allocated for the node state itself - that's handled by the memory context system
- Located at src/backend/executor/nodeBitmapIndexscan.c:175-201