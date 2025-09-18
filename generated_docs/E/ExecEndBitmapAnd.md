# ExecEndBitmapAnd

## Location
src/backend/executor/nodeBitmapAnd.c: 178 - 200

## Overview
ExecEndBitmapAnd shuts down a BitmapAndState node by recursively ending all of its initialized subplan nodes during query cleanup.

## Definition


## Detailed Description
ExecEndBitmapAnd performs cleanup operations for BitmapAnd executor nodes during query termination or plan tree teardown. It iterates through all subplan states stored in the BitmapAndState structure and calls ExecEndNode on each initialized subplan to ensure proper resource cleanup.

The function includes a safety check to only call ExecEndNode on subplans that have been successfully initialized (non-NULL pointers), preventing errors if initialization was incomplete or failed partway through. This defensive approach ensures robust cleanup even in error conditions.

As a cleanup function, ExecEndBitmapAnd does not return any value and focuses solely on resource deallocation and proper termination of child nodes. It is part of the standard executor node lifecycle, being called during the cleanup phase after query execution completes.

## Parameters / Member Variables
- : Pointer to the BitmapAndState containing the subplan nodes to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - ExecEndNode (to recursively end each subplan)
- Called from (representative examples):
  - ExecEndNode (general node cleanup dispatcher)

## Notes and Other Information
- Part of the standard PostgreSQL executor node cleanup lifecycle
- Includes defensive programming to handle partially initialized nodes
- Does not perform any complex cleanup since BitmapAnd nodes don't allocate expression contexts or tuple slots
- Ensures all child nodes are properly terminated to prevent resource leaks
- Called during query cleanup, plan tree destruction, or error handling
- Located in src/backend/executor/nodeBitmapAnd.c:178-200