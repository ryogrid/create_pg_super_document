# ExecEvalExprSwitchContext

## Location
src/include/executor/executor.h: 349 - 376

## Overview
ExecEvalExprSwitchContext evaluates an SQL expression within a specific memory context, ensuring proper memory management during expression evaluation by switching to the per-tuple memory context.

## Definition


## Detailed Description
ExecEvalExprSwitchContext provides a context-aware version of expression evaluation that temporarily switches to the per-tuple memory context before evaluating the expression, then restores the previous context. This is crucial for memory management in PostgreSQL because it ensures that any memory allocations performed during expression evaluation are allocated in the appropriate context (typically the per-tuple context that gets reset after each tuple is processed).

The function saves the current memory context, switches to the econtext's per-tuple memory context, performs the expression evaluation using the compiled evaluation function, then restores the original context before returning the result. This pattern prevents memory leaks and ensures that temporary allocations during expression evaluation are properly cleaned up.

## Parameters / Member Variables
- : ExprState containing the compiled expression evaluation function and associated metadata
- : ExprContext providing access to current tuple data, parameters, and the target per-tuple memory context
- : Pointer to bool that will be set to indicate whether the evaluated result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo (for switching memory contexts)
  - state->evalfunc (the compiled expression evaluation function)
- Called from (representative examples):
  - FormIndexDatum (for index key formation)
  - ExecuteCallStmt (for function call execution)
  - ExecCheck (for CHECK constraint evaluation)
  - FormPartitionKeyDatum (for partition key computation)
  - advance_aggregates (in aggregate processing)
  - ExecProject (for projection operations)
  - ExecQual (for qualification testing)

## Notes and Other Information
- This function is essential for preventing memory leaks in expression evaluation by ensuring allocations occur in the correct memory context
- The per-tuple memory context (econtext->ecxt_per_tuple_memory) is typically reset after each tuple is processed, automatically freeing any temporary allocations
- This context switching is particularly important for complex expressions that may allocate temporary memory for intermediate results
- The function maintains the same interface as ExecEvalExpr but adds the critical memory management aspect
- Static inline definition ensures efficient execution while providing the memory safety benefits
- Used extensively throughout the executor when expression evaluation needs to be performed with proper memory context management