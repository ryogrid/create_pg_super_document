# ExecEndMergeJoin

## Location
src/backend/executor/nodeMergejoin.c: 1641 - 1656

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
  - ExecEndNode (to recursively shut down child nodes)
  - MJ1_printf (debug logging)
- Called from (representative examples):
  - ExecEndNode (executor node termination dispatcher)

## Notes and Other Information
- Does not explicitly free memory as PostgreSQL uses memory contexts for automatic cleanup
- Relies on recursive ExecEndNode calls to properly shut down the entire plan subtree
- Part of the standard executor node lifecycle: Init -> Execute -> End
- The function does not return any value as cleanup operations are expected to succeed
- Debug logging provides visibility into the shutdown process when enabled
- Critical for proper resource management in long-running transactions or when handling query cancellation
- Must be called to prevent resource leaks when query execution terminates