# CreateExprContext

## Location
[src/backend/executor/execUtils.c:304-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L304-L318)

## Overview
Creates a standard ExprContext for expression evaluation within an EState, using default memory allocation parameters.

## Definition

```c
ExprContext *
CreateExprContext(EState *estate)
```
## Detailed Description
CreateExprContext is a public interface function that creates a standard ExprContext for expression evaluation. It serves as a convenient wrapper around CreateExprContextInternal(), using default AllocSet memory management parameters (ALLOCSET_DEFAULT_SIZES). This function is typically used when creating ExprContexts for Plan nodes and per-output-tuple processing such as constraint checking.

An executor run may require multiple ExprContexts, with each having its own "per-tuple" memory context for temporary allocations during expression evaluation. The function makes no assumptions about the caller's memory context and handles all necessary context switching internally.

## Parameters / Member Variables
- : Pointer to the EState that will own this ExprContext

The function delegates all initialization work to CreateExprContextInternal() with standard parameters:
- Uses ALLOCSET_DEFAULT_SIZES for minContextSize, initBlockSize, and maxBlockSize

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExprContextInternal](CreateExprContextInternal.md)
  - ALLOCSET_DEFAULT_SIZES

- Called from (representative examples):
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md)
  - [MakePerTupleExprContext](../M/MakePerTupleExprContext.md)
  - [ExecAssignExprContext](../E/ExecAssignExprContext.md)
  - [ExecInitMergeJoin](../E/ExecInitMergeJoin.md)
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - do_text_output_oneline

## Notes and Other Information
This is the standard public interface for creating ExprContexts in PostgreSQL's executor. It provides a simple, one-parameter interface while hiding the complexity of memory management parameter tuning. For scenarios requiring custom memory allocation behavior, callers would use CreateWorkExprContext() instead. The function is commonly used throughout the executor infrastructure, with many Plan node initialization routines calling it to set up expression evaluation contexts. Each ExprContext created gets its own per-tuple memory context that can be reset between tuple evaluations to manage memory usage efficiently.

## Simplified Source

```c
ExprContext *
CreateExprContext(EState *estate)
{
    return CreateExprContextInternal(estate, ALLOCSET_DEFAULT_SIZES);
}
```