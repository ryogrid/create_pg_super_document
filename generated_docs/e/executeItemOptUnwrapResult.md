# executeItemOptUnwrapResult

## Location
[src/backend/utils/adt/jsonpath_exec.c:1725-1759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L1725-L1759)

## Overview
This function executes a JSONPath item with optional automatic array unwrapping in lax mode, processing each array item individually when unwrapping is enabled.

## Definition

```c
static JsonPathExecResult
executeItemOptUnwrapResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb, bool unwrap,
						   JsonValueList *found)
```
## Detailed Description
The  function extends the basic  functionality by providing conditional array unwrapping in lax mode. When unwrapping is enabled and auto-unwrap conditions are met, it first executes the JSONPath item to collect results into a temporary sequence, then processes each result individually. If a result is an array, it calls  to handle the array elements; otherwise, it directly appends the result to the found list. This enables automatic flattening of nested arrays in lax mode operations.

## Parameters / Member Variables
- : JSONPath execution context containing state and configuration
- : JSONPath item representing the path expression to execute
- : JsonbValue containing the input data to process
- : Boolean flag indicating whether unwrapping should be attempted
- : List to collect final results after processing

## Dependencies
- Functions called/Symbols referenced:
  - jspAutoUnwrap
  - [executeItem](executeItem.md)
  - jperIsError
  - [JsonValueListInitIterator](../J/JsonValueListInitIterator.md)
  - [JsonValueListNext](../J/JsonValueListNext.md)
  - [JsonbType](../J/JsonbType.md)
  - [executeItemUnwrapTargetArray](executeItemUnwrapTargetArray.md)
  - [JsonValueListAppend](../J/JsonValueListAppend.md)
  - jperOk (enum value)
  - [JsonValueList](../J/JsonValueList.md) (type)
  - [JsonValueListIterator](../J/JsonValueListIterator.md) (type)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (return type)
  - jbvArray (enum value)
- Called from (representative examples):
  - [executeItemOptUnwrapResultNoThrow](executeItemOptUnwrapResultNoThrow.md)
  - [executeBinaryArithmExpr](executeBinaryArithmExpr.md)
  - [executeUnaryArithmExpr](executeUnaryArithmExpr.md)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c
- The function implements conditional logic based on both the unwrap parameter and jspAutoUnwrap() context check
- Uses a temporary JsonValueList to collect intermediate results before unwrapping
- Includes an assertion that items should not be of jbvArray type after initial processing
- Falls back to standard executeItem behavior when unwrapping conditions are not met
- Critical for implementing PostgreSQL's JSONPath lax mode semantics where arrays are automatically unwrapped in certain contexts
- Error handling preserves error states from the underlying executeItem call

## Simplified Source

```c
static JsonPathExecResult
executeItemOptUnwrapResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
                           JsonbValue *jb, bool unwrap,
                           JsonValueList *found) {
    // Check if unwrapping is needed and enabled
    if (unwrap && jspAutoUnwrap(cxt)) {
        JsonValueList seq = {0};
        JsonValueListIterator it;

        // Execute item and collect results in temporary sequence
        JsonPathExecResult res = executeItem(cxt, jsp, jb, &seq);
        if (jperIsError(res))
            return res;

        // Process each result, unwrapping arrays
        JsonValueListInitIterator(&seq, &it);
        JsonbValue *item;
        while ((item = JsonValueListNext(&seq, &it))) {
            if (JsonbType(item) == jbvArray) {
                // Unwrap array elements
                executeItemUnwrapTargetArray(cxt, NULL, item, found, false);
            } else {
                // Add non-array items directly
                JsonValueListAppend(found, item);
            }
        }
        return jperOk;
    }

    // No unwrapping needed - use standard execution
    return executeItem(cxt, jsp, jb, found);
}
```