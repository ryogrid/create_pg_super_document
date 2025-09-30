# CreateExprContextInternal

## Location
[src/backend/executor/execUtils.c:234-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L234-L303)

## Overview
Internal implementation function that creates and initializes an ExprContext node with configurable AllocSet parameters for memory management.

## Definition

```c
static ExprContext *
CreateExprContextInternal(EState *estate, Size minContextSize,
						  Size initBlockSize, Size maxBlockSize)
```
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
  - [lcons](../l/lcons.md)
  - makeNode
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)

- Called from (representative examples):
  - [CreateExprContext](CreateExprContext.md)
  - [CreateWorkExprContext](CreateWorkExprContext.md)

## Notes and Other Information
This is a static (internal) function that provides the flexibility to create ExprContexts with different memory management characteristics. The function uses lcons() to prepend the new ExprContext to the estate's list, which means that shutdown will occur in reverse order of creation during cleanup. The per-tuple memory context created here will be used for temporary allocations during expression evaluation and can be reset between tuple evaluations to reclaim memory. The function ensures proper memory context management by switching to the query context before allocation and restoring the previous context before returning.

## Simplified Source

```c
static ExprContext *
CreateExprContextInternal(EState *estate, Size minContextSize,
                          Size initBlockSize, Size maxBlockSize)
{
    ExprContext *econtext;
    MemoryContext oldcontext;

    // Create ExprContext in per-query memory context
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    econtext = makeNode(ExprContext);

    // Initialize tuple slots to NULL
    econtext->ecxt_scantuple = NULL;
    econtext->ecxt_innertuple = NULL;
    econtext->ecxt_outertuple = NULL;

    // Set memory contexts
    econtext->ecxt_per_query_memory = estate->es_query_cxt;
    econtext->ecxt_per_tuple_memory =
        AllocSetContextCreate(estate->es_query_cxt,
                              "ExprContext",
                              minContextSize,
                              initBlockSize,
                              maxBlockSize);

    // Link to estate parameters
    econtext->ecxt_param_exec_vals = estate->es_param_exec_vals;
    econtext->ecxt_param_list_info = estate->es_param_list_info;

    // Initialize aggregate and CASE/domain value fields
    econtext->ecxt_aggvalues = NULL;
    econtext->ecxt_aggnulls = NULL;
    econtext->caseValue_datum = (Datum) 0;
    econtext->caseValue_isNull = true;
    econtext->domainValue_datum = (Datum) 0;
    econtext->domainValue_isNull = true;

    // Set estate backpointer and callbacks
    econtext->ecxt_estate = estate;
    econtext->ecxt_callbacks = NULL;

    // Add to estate's context list for cleanup
    estate->es_exprcontexts = lcons(econtext, estate->es_exprcontexts);

    MemoryContextSwitchTo(oldcontext);
    return econtext;
}
```