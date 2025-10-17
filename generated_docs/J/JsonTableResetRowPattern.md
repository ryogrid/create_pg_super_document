# JsonTableResetRowPattern

## Location
[src/backend/utils/adt/jsonpath_exec.c:4253-4292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4253-L4292)

## Overview
Evaluates a JsonTablePlan's jsonpath expression against a given context item to generate a new row pattern and resets the plan state for iteration.

## Definition
```c
static void JsonTableResetRowPattern(JsonTablePlanState *planstate, Datum item)
```

## Detailed Description
JsonTableResetRowPattern is a static function that performs jsonpath evaluation and row pattern reset for JSON_TABLE processing. The function executes the following sequence:

1. **Memory Management**: Clears the existing found values list and resets the plan's memory context to free any previously allocated memory.

2. **Context Switching**: Switches to the plan's dedicated memory context to ensure proper memory allocation tracking.

3. **JsonPath Execution**: Executes the jsonpath expression stored in the plan against the provided JSON item using:
   - The plan's jsonpath expression
   - Variable arguments for parameterized queries
   - GetJsonPathVar and CountJsonPathVars for variable resolution
   - Error handling based on the plan's errorOnError flag

4. **Error Handling**: If jsonpath execution results in an error and errorOnError is false, clears the found values list to indicate no matches.

5. **Iterator Reset**: Initializes the iterator to the beginning of the found values list and resets the current row state to null/invalid with ordinal 0.

The function essentially prepares a fresh execution state for iterating through rows matching the jsonpath pattern.

## Parameters / Member Variables
- `planstate`: JsonTablePlanState pointer containing the plan execution state to reset
- `item`: Datum containing the JSON item (context) against which to evaluate the jsonpath

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for JsonTablePathScan type casting)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md) (JSON extraction from Datum)
  - [JsonValueListClear](JsonValueListClear.md) (value list cleanup)
  - [MemoryContextResetOnly](../M/MemoryContextResetOnly.md)/MemoryContextSwitchTo (memory management)
  - [executeJsonPath](../e/executeJsonPath.md) (core jsonpath execution)
  - [GetJsonPathVar](../G/GetJsonPathVar.md)/CountJsonPathVars (variable resolution)
  - jperIsError (error checking)
  - [JsonValueListInitIterator](JsonValueListInitIterator.md) (iterator initialization)
  - [PointerGetDatum](../P/PointerGetDatum.md) (NULL pointer conversion)
- Called from (representative examples):
  - [JsonTableSetDocument](JsonTableSetDocument.md) (document-level reset)
  - [JsonTableResetNestedPlan](JsonTableResetNestedPlan.md) (nested plan reset)

## Notes and Other Information
- This is a static function within jsonpath_exec.c, part of the JSON_TABLE execution infrastructure
- The function assumes the planstate contains a JsonTablePathScan plan (uses castNode assertion)
- Memory context switching ensures that jsonpath execution results are allocated in the correct context
- The function handles both successful execution and error cases gracefully
- Iterator reset ensures that subsequent calls to get next row will start from the beginning
- The ordinal counter is reset to 0, which is used for row numbering in JSON_TABLE
- Error handling depends on the plan's errorOnError setting - errors may be suppressed or propagated
- The function is essential for both initial document processing and nested plan resets

## Simplified Source

```c
static void
JsonTableResetRowPattern(JsonTablePlanState *planstate, Datum item)
{
    JsonTablePathScan *scan = castNode(JsonTablePathScan, planstate->plan);
    MemoryContext oldcxt;
    JsonPathExecResult res;
    Jsonb *js = (Jsonb *) DatumGetJsonbP(item);

    // Clear previous results and reset memory context
    JsonValueListClear(&planstate->found);
    MemoryContextResetOnly(planstate->mcxt);

    oldcxt = MemoryContextSwitchTo(planstate->mcxt);

    // Execute jsonpath against the JSON item
    res = executeJsonPath(planstate->path, planstate->args,
                         GetJsonPathVar, CountJsonPathVars,
                         js, scan->errorOnError,
                         &planstate->found,
                         true);

    MemoryContextSwitchTo(oldcxt);

    // Handle errors if errorOnError is false
    if (jperIsError(res)) {
        Assert(!scan->errorOnError);
        JsonValueListClear(&planstate->found);
    }

    // Reset iterator to beginning and clear current row state
    JsonValueListInitIterator(&planstate->found, &planstate->iter);
    planstate->current.value = PointerGetDatum(NULL);
    planstate->current.isnull = true;
    planstate->ordinal = 0;
}
```