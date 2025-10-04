# MakePerTupleExprContext

## Location
[src/backend/executor/execUtils.c:456-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L456-L482)

## Overview
Creates or returns the shared per-tuple ExprContext for an EState, providing a reusable context for per-tuple expression evaluation.

## Definition

```c
ExprContext *
MakePerTupleExprContext(EState *estate)
```
## Detailed Description
MakePerTupleExprContext implements a lazy initialization pattern for the shared per-tuple ExprContext within an EState. If the EState doesn't already have a per-tuple expression context (es_per_tuple_exprcontext is NULL), the function creates one using CreateExprContext. If the context already exists, it simply returns the existing instance.

This function provides a mechanism for sharing a single ExprContext across multiple operations within an executor state, which is more efficient than creating separate contexts for each operation. The shared context is particularly useful for evaluating expressions that don't require isolation between evaluations.

The function is typically accessed through the GetPerTupleExprContext() macro rather than being called directly, providing a convenient interface for obtaining the per-tuple context.

## Parameters / Member Variables
- `*estate`: The EState structure that will own or already owns the per-tuple ExprContext
## Dependencies
- Functions called/Symbols referenced:
  - [CreateExprContext](../C/CreateExprContext.md) (creates new ExprContext if none exists)

- Called from (representative examples):
  - ResetExprContext (via macro in src/include/executor/executor.h:548)
  - GetPerTupleExprContext (via macro in src/include/executor/executor.h:554)

## Notes and Other Information
- Implements lazy initialization - context is only created when first needed
- Provides a shared ExprContext for efficiency in per-tuple operations
- The created context is stored in estate->es_per_tuple_exprcontext for reuse
- Normally accessed through the GetPerTupleExprContext() macro, not called directly
- Part of PostgreSQL's memory management optimization for expression evaluation
- The returned context is tied to the EState's lifecycle and will be cleaned up when the EState is freed
- Suitable for expressions that don't require isolation between tuple evaluations

## Simplified Source

```c
ExprContext *
MakePerTupleExprContext(EState *estate)
{
    // Lazy initialization: create context only if needed
    if (estate->es_per_tuple_exprcontext == NULL)
        estate->es_per_tuple_exprcontext = CreateExprContext(estate);

    return estate->es_per_tuple_exprcontext;
}
```