# CreateExprContextInternal

## Location
src/backend/executor/execUtils.c: 234 - 303

## Overview
Internal implementation function that creates and initializes an ExprContext node with configurable AllocSet parameters for memory management.

## Definition


## Detailed Description
CreateExprContextInternal is a static helper function that provides the core implementation for creating ExprContext nodes. It serves as the common backend for both CreateExprContext() and CreateWorkExprContext(), allowing fine-grained control over the memory allocation parameters of the per-tuple memory context.

The function creates an ExprContext within the per-query memory context and initializes all its fields to appropriate default values. It establishes a per-tuple memory context using AllocSetContextCreate with the specified memory management parameters, which is used for temporary allocations during expression evaluation. The ExprContext is automatically linked into the EState's expression context list to ensure proper cleanup when the EState is freed.

## Parameters / Member Variables
- : Pointer to the EState that will own this ExprContext
- : Minimum size for the AllocSet context
- : Initial block size for the AllocSet context  
- : Maximum block size for the AllocSet context

Key ExprContext fields initialized:
- , , : Set to NULL (no tuples initially)
- : Set to estate's query memory context
- : Created as new AllocSet context with specified parameters
- , : Inherited from estate
- , : Set to NULL (no aggregation data)
- , : Set to 0 with NULL flags
- : Backpointer to the owning EState
- : Set to NULL (no shutdown callbacks)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - lcons
  - makeNode
  - MemoryContextSwitchTo

- Called from (representative examples):
  - CreateExprContext
  - CreateWorkExprContext

## Notes and Other Information
This is a static (internal) function that provides the flexibility to create ExprContexts with different memory management characteristics. The function uses lcons() to prepend the new ExprContext to the estate's list, which means that shutdown will occur in reverse order of creation during cleanup. The per-tuple memory context created here will be used for temporary allocations during expression evaluation and can be reset between tuple evaluations to reclaim memory. The function ensures proper memory context management by switching to the query context before allocation and restoring the previous context before returning.