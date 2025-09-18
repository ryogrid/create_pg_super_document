# ExecEndNestLoop

## Location
src/backend/executor/nodeNestloop.c: 361 - 380

## Overview
ExecEndNestLoop cleans up and terminates a nested loop join node by closing down child plan nodes and freeing allocated resources.

## Definition
```c
void ExecEndNestLoop(NestLoopState *node)
```

## Detailed Description
ExecEndNestLoop is responsible for the proper cleanup and termination of a nested loop join execution node. This function is called when the nested loop join operation is complete or when the query execution is being terminated.

The function performs cleanup by recursively calling ExecEndNode on both the outer and inner child plan nodes. This ensures that all resources allocated by the child nodes, including memory, file handles, and other system resources, are properly released. The recursive nature of this cleanup ensures that the entire plan tree rooted at this nested loop node is properly terminated.

The function uses debug tracing through NL1_printf macros to log the beginning and completion of the cleanup process, which is useful for debugging and monitoring query execution.

## Parameters / Member Variables
- `node`: The NestLoopState containing the runtime state and child plan references to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md): Recursively terminates outer and inner child plan nodes
  - outerPlanState: Accesses the outer child plan state for cleanup
  - innerPlanState: Accesses the inner child plan state for cleanup
  - NL1_printf: Debug tracing macro for logging cleanup progress
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md): As part of plan tree cleanup process

## Notes and Other Information
- Uses NL1_printf debug macros for tracing cleanup operations
- Does not explicitly free the NestLoopState structure itself - that is handled by the memory context system
- Part of the standard node lifecycle: Init -> Execute -> End
- Ensures proper resource cleanup in case of query cancellation or completion
- Simple cleanup function that relies on recursive child node termination