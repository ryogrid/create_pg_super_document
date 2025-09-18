# ExecEndIndexOnlyScan

## Location
[src/backend/executor/nodeIndexonlyscan.c:398-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L398-L432)

## Overview
ExecEndIndexOnlyScan performs cleanup operations when an index-only scan node is terminated, releasing all resources associated with the scan including buffer pins, index scan descriptors, and index relations.

## Definition
```c
void ExecEndIndexOnlyScan(IndexOnlyScanState *node)
```

## Detailed Description
This function is the cleanup handler for index-only scan operations in PostgreSQL's executor. It systematically releases all resources that were allocated during the index-only scan lifecycle. The function extracts the index relation descriptor and index scan descriptor from the node state, releases any visibility map buffer pins that may be held, terminates the active index scan, and closes the index relation. This cleanup is essential to prevent resource leaks and ensure proper transaction cleanup.

The function follows PostgreSQL's standard cleanup pattern by checking for valid resources before attempting to release them, making it safe to call even if some resources were never allocated or have already been released.

## Parameters / Member Variables
- `node`: Pointer to the IndexOnlyScanState containing the execution state and resources to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseBuffer
  - [index_endscan](../i/index_endscan.md)
  - [index_close](../i/index_close.md)
- Types used:
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [IndexScanDesc](../I/IndexScanDesc.md)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md)

## Notes and Other Information
- This function is part of the standard executor node cleanup protocol
- It safely handles cases where resources may not have been allocated (null checks)
- The visibility map buffer release is particularly important for index-only scans as they rely heavily on visibility map information
- The index relation is closed with NoLock, indicating that lock management is handled elsewhere in the transaction lifecycle