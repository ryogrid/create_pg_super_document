# ExecEndAppend

## Location
[src/backend/executor/nodeAppend.c:386-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L386-L405)

## Overview
Cleanup function that shuts down all subplans of an Append node and releases associated resources.

## Definition

```c
void
ExecEndAppend(AppendState *node)
```
## Detailed Description
ExecEndAppend is the cleanup and termination function for PostgreSQL's Append node executor. It performs a straightforward but essential task of recursively calling ExecEndNode on each initialized subplan to ensure proper resource cleanup and memory deallocation.

The function operates by:
1. **Resource Extraction**: Gets the array of subplan PlanState pointers and the count of initialized plans
2. **Recursive Cleanup**: Iterates through all subplans and calls ExecEndNode on each one
3. **Memory Management**: Relies on the PostgreSQL memory context system for automatic memory cleanup

This function is part of the standard executor cleanup protocol and ensures that all resources allocated during the execution of the Append node and its subplans are properly released.

## Parameters / Member Variables
- : The AppendState containing the initialized subplans that need to be terminated

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md) (for recursive cleanup of each subplan)
- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (main executor cleanup dispatcher)

## Notes and Other Information
- The function does not explicitly handle async-specific cleanup as that is managed by the PostgreSQL async execution framework
- Memory cleanup is handled automatically by PostgreSQL's memory context system
- The function is designed to be safe to call even if some subplans failed during initialization
- No return value as cleanup operations are expected to always succeed
- The function follows the standard PostgreSQL executor cleanup pattern of recursively terminating child nodes