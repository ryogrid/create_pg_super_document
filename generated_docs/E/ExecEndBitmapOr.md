# ExecEndBitmapOr

## Location
[src/backend/executor/nodeBitmapOr.c:196-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapOr.c#L196-L218)

## Overview
ExecEndBitmapOr performs cleanup operations for a BitmapOr node by shutting down all initialized child subplans.

## Definition

```c
void
ExecEndBitmapOr(BitmapOrState *node)
```
## Detailed Description
ExecEndBitmapOr is responsible for the cleanup phase of BitmapOr node execution. The function iterates through all child subplan states stored in the BitmapOrState structure and calls ExecEndNode on each initialized subplan to perform proper resource cleanup and finalization.

The function includes a safety check to ensure that only initialized subplans (non-NULL entries in the bitmapplans array) are shut down. This prevents potential errors if some subplans were not successfully initialized during the ExecInitBitmapOr phase.

As a cleanup function, it does not return any value and focuses solely on resource deallocation and proper shutdown of the execution tree.

## Parameters / Member Variables
- : Pointer to the BitmapOrState structure containing the child subplan states to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEndNode](ExecEndNode.md) (recursively shuts down child subplans)

- Called from (representative examples):
  - [ExecEndNode](ExecEndNode.md) (part of the general node cleanup dispatch system)

## Notes and Other Information
- Performs null pointer checks before calling ExecEndNode on each child
- Does not deallocate the BitmapOrState structure itself, only cleans up child resources
- Must be called to prevent resource leaks when tearing down query execution trees
- Follows the standard PostgreSQL pattern of recursive cleanup through the execution tree
- The function is void and returns nothing, as documented in the source comments

## Simplified Source

```c
void ExecEndBitmapOr(BitmapOrState *node)
{
    // Get child plan information
    PlanState **bitmapplans = node->bitmapplans;
    int nplans = node->nplans;

    // Shut down each initialized child subplan
    for (int i = 0; i < nplans; i++)
    {
        if (bitmapplans[i])
            ExecEndNode(bitmapplans[i]);
    }
}
```