# ExecEndCteScan

## Location
[src/backend/executor/nodeCtescan.c:288-306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCtescan.c#L288-L306)

## Overview
ExecEndCteScan performs cleanup for a CteScanState node, specifically freeing the shared tuplestore if this node is the leader among multiple CTE scan instances.

## Definition


## Detailed Description
ExecEndCteScan handles the cleanup phase of CTE scan execution by implementing a leader-only cleanup strategy. Since multiple CTE scan nodes can share the same underlying tuplestore (created during the leader election process in ExecInitCteScan), only the leader node is responsible for freeing the shared tuplestore resource.

The function performs a simple but critical check:
1. **Leader Check**: Determines if this node is the leader by comparing node->leader with the node itself
2. **Tuplestore Cleanup**: If this is the leader, calls tuplestore_end() to free the shared tuplestore and sets the pointer to NULL
3. **Follower Handling**: Follower nodes do nothing, as they only held read pointers into the leader's tuplestore

This design ensures that the shared tuplestore is freed exactly once, regardless of how many CTE scan nodes were using it, and prevents double-free errors that could occur if all nodes attempted cleanup.

## Parameters / Member Variables
- : CteScanState to be cleaned up, containing leader information and potentially the shared tuplestore

## Dependencies
- Functions called/Symbols referenced:
  - [tuplestore_end](../t/tuplestore_end.md): Free tuplestore and its associated memory
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md): Called during plan tree cleanup phase

## Notes and Other Information
- Only the leader node performs actual cleanup; follower nodes are no-ops
- The leader is identified by the condition (node->leader == node)
- Critical for proper memory management in multi-CTE-scan scenarios
- Follower nodes' read pointers are automatically cleaned up when the tuplestore is freed
- Sets cte_table to NULL after freeing to prevent dangling pointer issues
- Located at src/backend/executor/nodeCtescan.c:288-306