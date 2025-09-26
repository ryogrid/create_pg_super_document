# ExecShutdownHashJoin

## Location
[src/backend/executor/nodeHashjoin.c:1483-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1483-L1497)

## Overview
Performs cleanup operations when shutting down a hash join node by detaching from shared memory structures before they are destroyed.

## Definition
void ExecShutdownHashJoin(HashJoinState *node)

## Detailed Description
ExecShutdownHashJoin is responsible for properly cleaning up a hash join node during shutdown. Its primary function is to ensure that the node is safely detached from any shared hash table state before the associated dynamic shared memory (DSM) segments are freed. This prevents dangling pointers and ensures proper resource cleanup in parallel hash join operations.

The function performs two critical detachment operations:
1. Detaches from the current batch using ExecHashTableDetachBatch()
2. Detaches from the overall hash table using ExecHashTableDetach()

This shutdown process is essential for parallel hash joins where multiple worker processes may be sharing hash table data through DSM segments.

## Parameters / Member Variables
- `node`: Pointer to the HashJoinState structure representing the hash join execution state

## Dependencies
- Functions called/Symbols referenced:
  - ExecHashTableDetachBatch
  - ExecHashTableDetach
  - HashJoinState (struct type)
- Called from (representative examples):
  - ExecShutdownNode_walker

## Notes and Other Information
- This function is specifically designed for parallel hash join cleanup scenarios
- The detachment operations ensure no pointers into DSM memory remain by the time ExecEndHashJoin executes
- The function only performs cleanup if a hash table exists (node->hj_HashTable is not NULL)
- Part of the PostgreSQL executor's node shutdown infrastructure for proper resource management