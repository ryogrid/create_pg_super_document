# ExecPartitionCheck

## Location
[src/backend/executor/execMain.c:1794-1846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1794-L1846)

## Overview
Validates that a tuple meets the partition constraint for a given result relation, optionally emitting an error if the constraint fails.

## Definition
```c
bool ExecPartitionCheck(ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                       EState *estate, bool emitError)
```

## Detailed Description
ExecPartitionCheck is a core function in PostgreSQL's partition constraint validation system. It evaluates whether a tuple satisfies the partition constraint of a target relation. The function performs lazy initialization of the partition check expression on first invocation, preparing and caching the expression state tree for subsequent evaluations. The constraint evaluation treats NULL results as success, following PostgreSQL's constraint handling conventions. When a constraint violation occurs and error emission is requested, the function delegates to ExecPartitionCheckEmitError for detailed error reporting.

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo structure containing relation metadata and cached partition check expression
- `slot`: TupleTableSlot containing the tuple to be validated against the partition constraint
- `estate`: Execution state containing query context and per-tuple expression evaluation context
- `emitError`: Boolean flag indicating whether to emit an error on constraint violation or simply return false

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionQual](../R/RelationGetPartitionQual.md)
  - [ExecPrepareCheck](ExecPrepareCheck.md)
  - GetPerTupleExprContext
  - [ExecCheck](ExecCheck.md)
  - [ExecPartitionCheckEmitError](ExecPartitionCheckEmitError.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecBRInsertTriggers](ExecBRInsertTriggers.md)
  - [ExecFindPartition](ExecFindPartition.md)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md)
  - [ExecInsert](ExecInsert.md)
  - [ExecUpdateAct](ExecUpdateAct.md)

## Notes and Other Information
- The function implements lazy initialization, building the partition check expression only on first access
- Memory context management ensures the prepared expression persists for the query lifetime
- NULL constraint evaluation results are treated as success, consistent with cataloged constraint handling
- The function is critical for maintaining partition constraint integrity during INSERT and UPDATE operations
- Error handling is delegated to ExecPartitionCheckEmitError when constraint violations occur and error emission is requested

## Simplified Source

```c
// Simplified version of ExecPartitionCheck
bool ExecPartitionCheck(ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                       EState *estate, bool emitError) {
    ExprContext *econtext;
    bool success;

    // First-time initialization: build partition check expression
    if (resultRelInfo->ri_PartitionCheckExpr == NULL) {
        // Switch to query-lifespan memory context for persistence
        MemoryContext oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

        // Get partition constraint and prepare expression
        List *qual = RelationGetPartitionQual(resultRelInfo->ri_RelationDesc);
        resultRelInfo->ri_PartitionCheckExpr = ExecPrepareCheck(qual, estate);

        MemoryContextSwitchTo(oldcxt);
    }

    // Set up expression evaluation context
    econtext = GetPerTupleExprContext(estate);
    econtext->ecxt_scantuple = slot;

    // Evaluate partition constraint (NULL treated as success)
    success = ExecCheck(resultRelInfo->ri_PartitionCheckExpr, econtext);

    // Handle constraint violation
    if (!success && emitError) {
        ExecPartitionCheckEmitError(resultRelInfo, slot, estate);
    }

    return success;
}
```

Key simplifications made:
- Removed detailed comments explaining corner cases
- Simplified memory context switching explanation
- Consolidated the core logic flow into clear steps
- Preserved essential algorithm and error handling
- Maintained all critical function calls and variable usage