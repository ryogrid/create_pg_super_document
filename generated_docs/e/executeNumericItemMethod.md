# executeNumericItemMethod

## Location
[src/backend/utils/adt/jsonpath_exec.c:2298-2338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2298-L2338)

## Overview
Executes numeric item methods (.abs(), .floor(), .ceil()) on JSON path expressions using a specified PostgreSQL function.

## Definition
```c
static JsonPathExecResult executeNumericItemMethod(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, bool unwrap, PGFunction func, JsonValueList *found)
```

## Detailed Description
The `executeNumericItemMethod` function provides a generic implementation for numeric operations in JSON path expressions. It handles numeric methods like .abs(), .floor(), and .ceil() by applying a user-specified PostgreSQL function to numeric values. The function validates that the input is numeric, handles array unwrapping when specified, converts the JsonbValue to a Datum, applies the numeric function, and processes the result for further JSON path evaluation.

## Parameters / Member Variables
- `cxt`: JsonPathExecContext pointer for execution context and error handling
- `jsp`: JsonPathItem pointer representing the current path item being processed
- `jb`: JsonbValue pointer to the input value to apply the numeric method on
- `unwrap`: boolean flag indicating whether to unwrap arrays before processing
- `func`: PGFunction pointer to the PostgreSQL numeric function to execute
- `found`: JsonValueList pointer for collecting matching results

## Dependencies
- Functions called/Symbols referenced:
  - [executeItemUnwrapTargetArray](executeItemUnwrapTargetArray.md): Handles array unwrapping logic
  - [getScalar](../g/getScalar.md): Converts JsonbValue to numeric scalar
  - [jspOperationName](../j/jspOperationName.md): Gets operation name for error messages
  - DirectFunctionCall1: Calls PostgreSQL function with single argument
  - [NumericGetDatum](../N/NumericGetDatum.md)/DatumGetNumeric: Convert between Numeric and Datum types
  - [jspGetNext](../j/jspGetNext.md): Gets next item in JSON path
  - [executeNextItem](executeNextItem.md): Continues JSON path execution
  - [JsonbType](../J/JsonbType.md): Gets type of JsonbValue
  - [palloc](../p/palloc.md): PostgreSQL memory allocation
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md): Main item execution dispatcher for numeric methods
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathExecResult indicating success (jperOk) or error conditions
- Only accepts numeric input values; throws error for non-numeric types
- Supports array unwrapping when unwrap parameter is true
- Creates new JsonbValue with result and continues path execution if more items exist
- Part of PostgreSQL's JSON path expression evaluation system for numeric operations
- Generic implementation allows reuse for multiple numeric methods (.abs, .floor, .ceil)

## Simplified Source

```c
static JsonPathExecResult
executeNumericItemMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
                         JsonbValue *jb, bool unwrap, PGFunction func,
                         JsonValueList *found)
{
    JsonPathItem next;
    Datum datum;

    // Handle array unwrapping if requested
    if (unwrap && JsonbType(jb) == jbvArray)
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

    // Validate input is numeric
    if (!(jb = getScalar(jb, jbvNumeric)))
        RETURN_ERROR(ereport(ERROR,
                    (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
                     errmsg("jsonpath item method .%s() can only be applied to a numeric value",
                            jspOperationName(jsp->type)))));

    // Apply the numeric function
    datum = DirectFunctionCall1(func, NumericGetDatum(jb->val.numeric));

    // If no next item and not collecting results, we're done
    if (!jspGetNext(jsp, &next) && !found)
        return jperOk;

    // Create result value and continue execution
    jb = palloc(sizeof(*jb));
    jb->type = jbvNumeric;
    jb->val.numeric = DatumGetNumeric(datum);

    return executeNextItem(cxt, jsp, &next, jb, found, false);
}
```