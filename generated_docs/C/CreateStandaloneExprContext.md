# CreateStandaloneExprContext

## Location
[src/backend/executor/execUtils.c:355-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L355-L413)

## Overview
Creates a standalone ExprContext for expression evaluation that operates independently of an executor state, suitable for evaluating expressions without Params, subplans, or Var references.

## Definition

```c
ExprContext *
CreateStandaloneExprContext(void)
```
## Detailed Description
CreateStandaloneExprContext creates an ExprContext structure designed for standalone expression evaluation scenarios where no executor state is available. Unlike regular ExprContexts created within an EState, this standalone version operates in isolation and uses the caller's current memory context as its "per query" context.

The function initializes all tuple slots (scan, inner, outer) to NULL, creates a dedicated working memory context for per-tuple allocations, and sets up default values for special expression evaluation fields like caseValue and domainValue. It explicitly excludes parameter handling and aggregate value storage since these features require executor state support.

The caller is responsible for proper cleanup, either by explicitly freeing the context or ensuring shutdown callbacks are executed via ReScanExprContext() to prevent resource leaks.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates the ExprContext node)
  - AllocSetContextCreate (creates per-tuple memory context)
  - ALLOCSET_DEFAULT_SIZES (memory allocation sizing constants)
  - CurrentMemoryContext (uses caller's memory context as per-query context)

- Called from (representative examples):
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md) (in src/backend/executor/execGrouping.c:239)
  - [domain_check_input](../d/domain_check_input.md) (in src/backend/utils/adt/domains.c:172)
  - [hypothetical_dense_rank_final](../h/hypothetical_dense_rank_final.md) (in src/backend/utils/adt/orderedsetaggs.c:1325)
  - do_text_output_oneline (via inline in src/include/executor/executor.h:541)

## Notes and Other Information
- The created ExprContext cannot handle Params, subplans, or Var references since it lacks an associated EState
- Tuple references might work if placed in the scantuple field, but this is discouraged
- The function is commonly used in utility functions that need to evaluate simple expressions outside the main executor framework
- Memory management follows PostgreSQL's standard pattern: per-query context for the structure itself, per-tuple context for temporary evaluation work
- Unlike CreateExprContext, this function does not require or associate with an EState structure

## Simplified Source

```c
ExprContext *
CreateStandaloneExprContext(void)
{
    ExprContext *econtext;

    // Create the ExprContext node in caller's memory context
    econtext = makeNode(ExprContext);

    // Initialize tuple slots to NULL (no tuple access in standalone mode)
    econtext->ecxt_scantuple = NULL;
    econtext->ecxt_innertuple = NULL;
    econtext->ecxt_outertuple = NULL;

    // Use caller's context as the per-query memory context
    econtext->ecxt_per_query_memory = CurrentMemoryContext;

    // Create dedicated working memory for expression evaluation
    econtext->ecxt_per_tuple_memory =
        AllocSetContextCreate(CurrentMemoryContext,
                              "ExprContext",
                              ALLOCSET_DEFAULT_SIZES);

    // Initialize parameter handling (NULL for standalone use)
    econtext->ecxt_param_exec_vals = NULL;
    econtext->ecxt_param_list_info = NULL;

    // Initialize aggregate handling (NULL for standalone use)
    econtext->ecxt_aggvalues = NULL;
    econtext->ecxt_aggnulls = NULL;

    // Initialize special expression values
    econtext->caseValue_datum = (Datum) 0;
    econtext->caseValue_isNull = true;
    econtext->domainValue_datum = (Datum) 0;
    econtext->domainValue_isNull = true;

    // No executor state for standalone context
    econtext->ecxt_estate = NULL;
    econtext->ecxt_callbacks = NULL;

    return econtext;
}
```