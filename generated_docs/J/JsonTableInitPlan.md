# JsonTableInitPlan

## Location
[src/backend/utils/adt/jsonpath_exec.c:4193-4239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4193-L4239)

## Overview
Recursively initializes JsonTablePlanState structures for evaluating jsonpath expressions in JsonTablePlan nodes and their child plans.

## Definition
```c
static JsonTablePlanState *JsonTableInitPlan(JsonTableExecContext *cxt, JsonTablePlan *plan, JsonTablePlanState *parentstate, List *args, MemoryContext mcxt)
```

## Detailed Description
JsonTableInitPlan is a recursive function that creates and initializes JsonTablePlanState structures for different types of JsonTablePlan nodes. It handles two main plan types:

1. **JsonTablePathScan**: For path scanning operations, it:
   - Extracts the jsonpath from the plan's path constant value
   - Creates a dedicated memory context for execution
   - Initializes current row pattern state (initially null)
   - Maps column indices to the plan state in the execution context
   - Recursively initializes any child plans

2. **JsonTableSiblingJoin**: For sibling join operations, it:
   - Recursively initializes both left and right child plans
   - Maintains the parent state relationship

The function builds a tree structure of plan states that mirrors the logical structure of the JSON_TABLE plan, enabling proper execution flow and context management.

## Parameters / Member Variables
- `cxt`: JsonTableExecContext pointer containing the overall execution context
- `plan`: JsonTablePlan pointer to the plan node being initialized
- `parentstate`: JsonTablePlanState pointer to the parent plan state (can be NULL for root)
- `args`: List pointer containing JsonPathVariable arguments for jsonpath evaluation
- `mcxt`: MemoryContext for memory allocation context

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - [DatumGetJsonPathP](../D/DatumGetJsonPathP.md) (jsonpath extraction from Datum)
  - AllocSetContextCreate (memory context creation)
  - [PointerGetDatum](../P/PointerGetDatum.md) (NULL pointer to Datum conversion)
  - IsA (type checking macro)
- Called from (representative examples):
  - [JsonTableInitOpaque](JsonTableInitOpaque.md) (root plan initialization)
  - [JsonTableInitPlan](JsonTableInitPlan.md) (recursive calls for child plans)

## Notes and Other Information
- This is a static recursive function within jsonpath_exec.c
- The function creates separate memory contexts for each JsonTablePathScan to manage memory efficiently
- Column mapping (colMin to colMax range) allows direct lookup of plan states by column index
- The function handles the tree structure of nested and sibling JSON table plans
- Initial row pattern state is set to null/invalid, indicating no pattern has been evaluated yet
- The recursive nature allows for complex nested JSON_TABLE structures with multiple levels
- Memory context naming uses "JsonTableExecContext" for easier debugging and memory tracking

## Simplified Source

```c
static JsonTablePlanState *
JsonTableInitPlan(JsonTableExecContext *cxt, JsonTablePlan *plan,
                  JsonTablePlanState *parentstate,
                  List *args, MemoryContext mcxt)
{
    JsonTablePlanState *planstate = palloc0(sizeof(*planstate));

    planstate->plan = plan;
    planstate->parent = parentstate;

    if (IsA(plan, JsonTablePathScan)) {
        JsonTablePathScan *scan = (JsonTablePathScan *) plan;
        int i;

        // Extract jsonpath and set up execution context
        planstate->path = DatumGetJsonPathP(scan->path->value->constvalue);
        planstate->args = args;
        planstate->mcxt = AllocSetContextCreate(mcxt, "JsonTableExecContext",
                                               ALLOCSET_DEFAULT_SIZES);

        // Initialize current row pattern (no pattern evaluated yet)
        planstate->current.value = PointerGetDatum(NULL);
        planstate->current.isnull = true;

        // Map columns to this plan state
        for (i = scan->colMin; i >= 0 && i <= scan->colMax; i++)
            cxt->colplanstates[i] = planstate;

        // Recursively initialize child plan if present
        planstate->nested = scan->child ?
            JsonTableInitPlan(cxt, scan->child, planstate, args, mcxt) : NULL;
    }
    else if (IsA(plan, JsonTableSiblingJoin)) {
        JsonTableSiblingJoin *join = (JsonTableSiblingJoin *) plan;

        // Recursively initialize left and right child plans
        planstate->left = JsonTableInitPlan(cxt, join->lplan, parentstate,
                                           args, mcxt);
        planstate->right = JsonTableInitPlan(cxt, join->rplan, parentstate,
                                            args, mcxt);
    }

    return planstate;
}
```