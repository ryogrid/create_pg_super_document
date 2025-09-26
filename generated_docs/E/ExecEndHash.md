# ExecEndHash

## Location
src/backend/executor/nodeHash.c: 413 - 431

## Overview
Cleanup routine for Hash node that shuts down the subplan and deallocates associated resources during query execution termination.

## Definition
void ExecEndHash(HashState *node)

## Detailed Description
ExecEndHash is a cleanup function responsible for properly terminating a Hash node during query execution shutdown. This function is part of PostgreSQL's executor framework and follows the standard pattern for ending plan nodes. It primarily focuses on shutting down the outer subplan that feeds data to the hash table, ensuring proper resource deallocation and cleanup when the hash operation is complete or aborted.

The function is called as part of the executor's cleanup phase when a query containing hash operations completes, whether successfully or due to an error. It ensures that all child plan nodes are properly terminated in a hierarchical manner.

## Parameters / Member Variables
- node: HashState pointer containing the hash operation's execution state, including references to the outer subplan and hash table structures

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (macro to access outer plan)
  - ExecEndNode (recursively shuts down child plan nodes)
- Called from (representative examples):
  - ExecEndNode (general node termination dispatcher)

## Notes and Other Information
- This function follows PostgreSQL's standard executor cleanup pattern where each node type has a corresponding ExecEnd* function
- The function does not directly clean up the hash table itself - that is handled elsewhere in the hash join cleanup process
- Part of the Hash node implementation in nodeHash.c, which supports hash operations in hash joins
- The cleanup is performed in a top-down manner, ensuring child nodes are properly terminated before parent nodes