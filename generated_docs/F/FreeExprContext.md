# FreeExprContext

## Location
src/backend/executor/execUtils.c: 414 - 440

## Overview
Properly frees an ExprContext structure, including executing shutdown callbacks and releasing all associated memory resources.

## Definition

```c
void
FreeExprContext(ExprContext *econtext, bool isCommit)
```
## Detailed Description
FreeExprContext performs complete cleanup of an ExprContext structure. It executes any registered shutdown callbacks through ShutdownExprContext, deletes the per-tuple memory context that was used for expression evaluation, unlinks the context from its owning EState (if any), and finally frees the ExprContext node itself.

The function handles both normal shutdown scenarios (when isCommit is true) and error cleanup scenarios (when isCommit is false). During error cleanup, callbacks may be skipped to avoid potential issues, but memory is still properly released.

An important consequence of this function is that any previously computed pass-by-reference expression results stored in the per-tuple memory context will be invalidated and freed.

## Parameters / Member Variables
- : The ExprContext structure to be freed
- : Boolean flag indicating whether this is a normal commit (true) or error cleanup (false). Affects whether shutdown callbacks are executed.

## Dependencies
- Functions called/Symbols referenced:
  - ShutdownExprContext (executes registered shutdown callbacks)
  - MemoryContextDelete (frees the per-tuple memory context)
  - list_delete_ptr (removes context from EState's context list)
  - pfree (frees the ExprContext node memory)

- Called from (representative examples):
  - FreeExecutorState (in src/backend/executor/execUtils.c:203)
  - do_text_output_oneline (via inline in src/include/executor/executor.h:542)

## Notes and Other Information
- This function invalidates any pass-by-reference expression results that were allocated in the per-tuple memory context
- The function makes no assumptions about the caller's current memory context
- During error cleanup (isCommit=false), shutdown callbacks may be skipped to prevent cascading errors
- Properly handles both standalone ExprContexts and those associated with an EState
- Essential for preventing memory leaks in PostgreSQL's expression evaluation system
- Should be paired with CreateExprContext or CreateStandaloneExprContext calls