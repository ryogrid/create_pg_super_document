# ExecEndIndexScan

## Location
src/backend/executor/nodeIndexscan.c: 785 - 812

## Overview
Terminates an index scan operation and properly releases all associated resources including the index scan descriptor and index relation.

## Definition


## Detailed Description
The `ExecEndIndexScan` function performs the cleanup and resource deallocation necessary when terminating an index scan operation. It is responsible for properly closing the index scan descriptor and the index relation, ensuring that all resources are released and locks are handled appropriately.

This function is part of the executor node lifecycle and is called when the index scan node is being shut down, either at the end of query execution or when the plan tree is being torn down. It ensures that the index scan infrastructure is properly cleaned up to prevent resource leaks.

The function safely handles cases where the index scan descriptor or index relation might not have been initialized, making it safe to call regardless of the initialization state of the node.

## Parameters
- `node`: Pointer to the IndexScanState structure containing the index scan state and associated resources to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - index_endscan
  - index_close
- Data types used:
  - IndexScanState
  - IndexScanDesc
  - Relation
- Constants used:
  - NoLock

## Called From
- ExecEndNode (src/backend/executor/execProcnode.c:635)

## Notes and Other Information
- Part of the standard executor node cleanup protocol
- Safely handles null descriptors and relations without error
- Uses NoLock when closing index relation, indicating locks are managed elsewhere
- Must be called to prevent resource leaks when index scan operations complete
- Complementary to ExecInitIndexScan which initializes the index scan state
- Does not directly free the IndexScanState node itself, only its internal resources
- Critical for proper resource management in long-running database sessions