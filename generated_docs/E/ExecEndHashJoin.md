# ExecEndHashJoin

## Location
[src/backend/executor/nodeHashjoin.c:859-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L859-L889)

## Overview
ExecEndHashJoin performs cleanup operations for a HashJoin node during query plan shutdown, destroying the hash table and recursively cleaning up child nodes.

## Definition


## Detailed Description
ExecEndHashJoin is the cleanup routine for HashJoin nodes in PostgreSQL's executor. It performs orderly shutdown of hash join resources to prevent memory leaks and ensure proper cleanup of associated structures.

The function performs two main cleanup operations:
1. **Hash Table Destruction**: If a hash table was created during execution, it calls ExecHashTableDestroy to free all memory associated with the hash table, including hash buckets, batch files, and associated data structures
2. **Child Node Cleanup**: Recursively calls ExecEndNode on both the outer and inner child nodes to ensure complete cleanup of the entire plan subtree

This cleanup is essential in PostgreSQL's memory management strategy, as hash joins can consume significant memory resources, particularly in multi-batch scenarios where temporary files may have been created.

## Parameters / Member Variables
- : The HashJoinState structure containing the hash join execution state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ExecHashTableDestroy: Destroys the hash table and frees associated memory
  - [ExecEndNode](ExecEndNode.md): Recursively cleans up child plan nodes
  - outerPlanState: Accesses the outer child plan state
  - innerPlanState: Accesses the inner child plan state

- Called from:
  - [ExecEndNode](ExecEndNode.md): General node cleanup dispatcher

## Notes and Other Information
The cleanup order is important - the hash table is destroyed first before cleaning up child nodes. This ensures that any references to child node data within the hash table are handled properly before the child nodes are destroyed.

The function safely handles cases where no hash table was created (node->hj_HashTable is NULL), which can occur if the query was terminated early or if certain optimizations prevented hash table creation.

This is a relatively simple cleanup function compared to the complexity of hash join initialization and execution, reflecting PostgreSQL's design principle of making resource cleanup straightforward and reliable.

Location: src/backend/executor/nodeHashjoin.c:859-889