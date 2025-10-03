# executeItemUnwrapTargetArray

## Location
[src/backend/utils/adt/jsonpath_exec.c:1674-1693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L1674-L1693)

## Overview
This function unwraps a current array item and executes a JSONPath expression for each of its elements.

## Definition

```c
static JsonPathExecResult
executeItemUnwrapTargetArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
							 JsonbValue *jb, JsonValueList *found,
							 bool unwrapElements)
```
## Detailed Description
The  function is a specialized JSONPath execution function that operates on array values. It validates that the provided JsonbValue represents a binary-encoded array and then delegates the actual execution to  with specific parameters optimized for array processing. The function ensures type safety by checking that the input is a binary type (not a direct array type) and throws an error for invalid types.

## Parameters / Member Variables
- `*cxt`: JSONPath execution context containing state and configuration
- `*jsp`: JSONPath item representing the path expression to execute
- `*jb`: JsonbValue containing the array data to process
- `*found`: List to collect matching values during execution
- `unwrapElements`: Boolean flag indicating whether to unwrap individual elements
## Dependencies
- Functions called/Symbols referenced:
  - [executeAnyItem](executeAnyItem.md)
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (type)
  - JsonPathItem (type)
  - [JsonValueList](../J/JsonValueList.md) (type)
  - jbvBinary (enum value)
  - jbvArray (enum value)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (return type)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md)
  - [executeItemOptUnwrapResult](executeItemOptUnwrapResult.md)
  - [executeNumericItemMethod](executeNumericItemMethod.md)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c
- The function performs strict type validation, expecting jbvBinary type and explicitly asserting that jbvArray type should not occur
- The function delegates to executeAnyItem with parameters (1, 1, 1, false, unwrapElements) which configure specific execution behavior for array unwrapping
- Error handling includes an elog(ERROR) call for invalid jsonb array value types

## Simplified Source

```c
static JsonPathExecResult
executeItemUnwrapTargetArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
                             JsonbValue *jb, JsonValueList *found,
                             bool unwrapElements)
{
    // Validate input is binary array
    if (jb->type != jbvBinary)
    {
        Assert(jb->type != jbvArray);
        elog(ERROR, "invalid jsonb array value type: %d", jb->type);
    }

    // Delegate to executeAnyItem with array-specific parameters:
    // - level=1, first=1, last=1: process only immediate array elements
    // - ignoreStructuralErrors=false: strict error handling
    // - unwrapNext=unwrapElements: control element unwrapping
    return executeAnyItem(cxt, jsp, jb->val.binary.data, found, 1, 1, 1,
                         false, unwrapElements);
}
```