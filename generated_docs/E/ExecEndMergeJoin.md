# ExecEndMergeJoin

## Location
[src/backend/executor/nodeMergejoin.c:1641-1656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L1641-L1656)

## Overview
Performs cleanup operations for a merge join node by recursively shutting down its child plan nodes and releasing associated resources.

## Definition
```c
void ExecEndMergeJoin(MergeJoinState *node)
```

## Detailed Description
ExecEndMergeJoin handles the termination phase of merge join execution by performing orderly shutdown of the merge join node and its child plans. This function is part of PostgreSQL's structured cleanup mechanism that ensures proper resource deallocation when query execution completes, encounters an error, or is canceled.

The function follows a simple but critical pattern: it recursively calls ExecEndNode on both the inner and outer child plan nodes, allowing the entire plan tree to be properly dismantled from the bottom up. This recursive cleanup ensures that any resources held by child nodes (such as open files, allocated memory, or locks) are properly released.

The merge join node itself relies on PostgreSQL's memory context management for most of its cleanup, as the MergeJoinState structure and its associated data are allocated in execution-specific memory contexts that are automatically freed when the query execution context is destroyed.

## Parameters / Member Variables
- `node`: Pointer to the MergeJoinState structure representing the merge join node to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - innerPlanState (to access inner child plan)
  - outerPlanState (to access outer child plan)
  - [ExecEndNode](ExecEndNode.md) (to recursively shut down child nodes)
  - MJ1_printf (debug logging)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (executor node termination dispatcher)

## Notes and Other Information
- Does not explicitly free memory as PostgreSQL uses memory contexts for automatic cleanup
- Relies on recursive ExecEndNode calls to properly shut down the entire plan subtree
- Part of the standard executor node lifecycle: Init -> Execute -> End
- The function does not return any value as cleanup operations are expected to succeed
- Debug logging provides visibility into the shutdown process when enabled
- Critical for proper resource management in long-running transactions or when handling query cancellation
- Must be called to prevent resource leaks when query execution terminates

## Simplified Source

```c
// Simplified version of ExecEndMergeJoin
void ExecEndMergeJoin(MergeJoinState *node) {
    // Cleanup step 1: Shutdown inner child plan recursively
    ExecEndNode(innerPlanState(node));

    // Cleanup step 2: Shutdown outer child plan recursively
    ExecEndNode(outerPlanState(node));

    // Note: Memory cleanup handled automatically by PostgreSQL's memory context system
}
```

Key simplifications made:
- Removed debug logging statements (MJ1_printf calls)
- Added explanatory comments for the two main cleanup steps
- Focused on the essential recursive shutdown pattern
- Added note about automatic memory management