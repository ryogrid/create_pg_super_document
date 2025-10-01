# executeAnyItem

## Location
[src/backend/utils/adt/jsonpath_exec.c:1934-2024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L1934-L2024)

## Overview
Implements the execution of multiple JSON path accessor nodes including jpiAny (.** accessor), jpiAnyKey (.* accessor), and jpiAnyArray ([*] accessor) by recursively iterating over JSON objects and arrays.

## Definition
```c
static JsonPathExecResult executeAnyItem(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbContainer *jbc, JsonValueList *found, uint32 level, uint32 first, uint32 last, bool ignoreStructuralErrors, bool unwrapNext)
```

## Detailed Description
This function is a comprehensive implementation that handles three different JSON path accessor types that involve iterating over multiple elements:

1. **jpiAny (.** accessor)**: Recursively searches through all levels of nested JSON structures
2. **jpiAnyKey (.* accessor)**: Iterates over all keys in JSON objects
3. **jpiAnyArray ([*] accessor)**: Iterates over all elements in JSON arrays

The function uses a JsonbIterator to traverse the JSON structure, applying level-based filtering (first/last parameters) and recursively calling itself for nested binary JSON values. It supports both collecting results in a found list and early termination when only existence checking is needed.

The implementation includes sophisticated error handling with optional structural error suppression and stack depth checking to prevent infinite recursion. It's a core component of the JSON path execution engine that enables wildcard and recursive descent operations.

## Parameters / Member Variables
- `cxt`: Pointer to the JSON path execution context containing evaluation state and configuration
- `jsp`: Pointer to the JSON path item representing the next expression to evaluate (can be NULL)
- `jbc`: Pointer to the JsonbContainer to iterate over
- `found`: Pointer to JsonValueList for collecting matching results (NULL for existence-only checks)
- `level`: Current recursion depth level
- `first`: Minimum level at which to start collecting results (PG_UINT32_MAX for leaves-only mode)
- `last`: Maximum level at which to collect results
- `ignoreStructuralErrors`: Boolean flag to suppress structural errors during evaluation
- `unwrapNext`: Boolean flag indicating whether to unwrap the next level

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md)
  - [JsonValueListAppend](../J/JsonValueListAppend.md)
  - [copyJsonbValue](../c/copyJsonbValue.md)
  - jperIsError
  - [executeAnyItem](executeAnyItem.md) (recursive call)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md)
  - [executeItemUnwrapTargetArray](executeItemUnwrapTargetArray.md)
  - [executeAnyItem](executeAnyItem.md) (recursive calls)

## Notes and Other Information
- This is a static function used only within the jsonpath_exec.c compilation unit
- Implements recursive descent with stack depth protection
- Supports three different wildcard accessor patterns in JSON path expressions
- The level-based filtering mechanism enables precise control over which nested levels are processed
- Critical for implementing SQL/JSON path wildcard functionality
- The function can operate in both result-collection mode and existence-check mode depending on the `found` parameter

## Simplified Source

```c
static JsonPathExecResult
executeAnyItem(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbContainer *jbc,
               JsonValueList *found, uint32 level, uint32 first, uint32 last,
               bool ignoreStructuralErrors, bool unwrapNext)
{
    JsonPathExecResult res = jperNotFound;
    JsonbIterator *it;
    int32 r;
    JsonbValue v;

    check_stack_depth();

    // Check level bounds
    if (level > last)
        return res;

    it = JsonbIteratorInit(jbc);

    // Recursively iterate over JSON objects/arrays
    while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
    {
        // Skip keys, process values and elements
        if (r == WJB_KEY)
        {
            r = JsonbIteratorNext(&it, &v, true);
            Assert(r == WJB_VALUE);
        }

        if (r == WJB_VALUE || r == WJB_ELEM)
        {
            // Check if current level should be processed
            if (level >= first ||
                (first == PG_UINT32_MAX && last == PG_UINT32_MAX && v.type != jbvBinary))
            {
                // Execute expression on current value
                if (jsp)
                {
                    res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);
                    if (jperIsError(res) || (res == jperOk && !found))
                        break;
                }
                else if (found)
                    JsonValueListAppend(found, copyJsonbValue(&v));
                else
                    return jperOk;
            }

            // Recurse into nested binary objects
            if (level < last && v.type == jbvBinary)
            {
                res = executeAnyItem(cxt, jsp, v.val.binary.data, found,
                                   level + 1, first, last,
                                   ignoreStructuralErrors, unwrapNext);
                if (jperIsError(res) || (res == jperOk && found == NULL))
                    break;
            }
        }
    }

    return res;
}
```