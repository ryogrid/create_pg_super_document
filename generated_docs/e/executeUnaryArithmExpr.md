# executeUnaryArithmExpr

## Location
[src/backend/utils/adt/jsonpath_exec.c:2176-2242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2176-L2242)

## Overview
Executes unary arithmetic expressions on each numeric item in the operand sequence with automatic array unwrapping in lax mode, supporting unary operations like plus and minus.

## Definition
```c
static JsonPathExecResult executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, PGFunction func, JsonValueList *found)
```

## Detailed Description
This function implements unary arithmetic operations for JSON path expressions by processing sequences of values:

1. **Operand Extraction**: Uses jspGetArg to extract the single operand from the JSON path item
2. **Sequence Evaluation**: Evaluates the operand with auto-unwrapping enabled to handle array operands in lax mode
3. **Sequential Processing**: Iterates through each item in the resulting sequence using JsonValueListIterator
4. **Type Validation**: Checks that each item is numeric using getScalar, with different behavior for missing results vs. found collections
5. **Arithmetic Application**: Applies the provided PGFunction to each numeric value using DirectFunctionCall1
6. **Result Chaining**: For each processed value, continues execution to any subsequent operations via executeNextItem

The function handles non-numeric values gracefully - it skips them when no results are being collected, but generates errors when results are expected. This allows for flexible processing of mixed-type sequences while maintaining type safety for arithmetic operations.

## Parameters / Member Variables
- `cxt`: Pointer to JSON path execution context containing mode settings and evaluation state
- `jsp`: Pointer to the JSON path item representing the unary arithmetic operation
- `jb`: Pointer to current JsonbValue context for operand evaluation  
- `func`: PGFunction pointer to the specific unary arithmetic operation (NULL for identity/plus operations)
- `found`: Pointer to JsonValueList for collecting results (NULL for existence-only checks)

## Dependencies
- Functions called/Symbols referenced:
  - [jspGetArg](../j/jspGetArg.md)
  - [executeItemOptUnwrapResult](executeItemOptUnwrapResult.md)
  - [jspGetNext](../j/jspGetNext.md)
  - [JsonValueListInitIterator](../J/JsonValueListInitIterator.md)
  - [JsonValueListNext](../J/JsonValueListNext.md)
  - [getScalar](../g/getScalar.md)
  - [jspOperationName](../j/jspOperationName.md)
  - DirectFunctionCall1
  - [DatumGetNumeric](../D/DatumGetNumeric.md)
  - [NumericGetDatum](../N/NumericGetDatum.md)
  - [executeNextItem](executeNextItem.md)
  - jperIsError
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md) (for unary plus and minus operations)

## Notes and Other Information
- This is a static function used only within the jsonpath_exec.c compilation unit
- Processes sequences of values rather than requiring singleton operands like binary arithmetic
- Uses PostgreSQL's function call interface (DirectFunctionCall1) for arithmetic operations
- Handles the identity operation when func is NULL (unary plus that doesn't modify the value)
- Non-numeric values are silently skipped in some contexts but cause errors when results are required
- Part of the JSON path arithmetic expression system supporting unary operators
- Automatic array unwrapping in lax mode enables intuitive arithmetic on array elements
- Each numeric value is processed independently, allowing for vectorized-style operations on sequences

## Simplified Source

```c
static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
                       JsonbValue *jb, PGFunction func, JsonValueList *found)
{
    JsonPathExecResult jper, jper2;
    JsonPathItem elem;
    JsonValueList seq = {0};
    JsonValueListIterator it;
    JsonbValue *val;
    bool hasNext;

    // Evaluate operand with auto-unwrapping
    jspGetArg(jsp, &elem);
    jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &seq);
    if (jperIsError(jper))
        return jper;

    jper = jperNotFound;
    hasNext = jspGetNext(jsp, &elem);

    // Process each value in the sequence
    JsonValueListInitIterator(&seq, &it);
    while ((val = JsonValueListNext(&seq, &it)))
    {
        // Validate value is numeric
        if ((val = getScalar(val, jbvNumeric)))
        {
            if (!found && !hasNext)
                return jperOk;
        }
        else
        {
            if (!found && !hasNext)
                continue;  // Skip non-numeric in some contexts

            RETURN_ERROR(ereport(ERROR,
                        (errcode(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND),
                         errmsg("operand of unary jsonpath operator %s is not a numeric value",
                                jspOperationName(jsp->type)))));
        }

        // Apply arithmetic function if provided (NULL for identity/unary plus)
        if (func)
            val->val.numeric = DatumGetNumeric(DirectFunctionCall1(func,
                                             NumericGetDatum(val->val.numeric)));

        // Continue execution with transformed value
        jper2 = executeNextItem(cxt, jsp, &elem, val, found, false);
        if (jperIsError(jper2))
            return jper2;

        if (jper2 == jperOk)
        {
            if (!found)
                return jperOk;
            jper = jperOk;
        }
    }

    return jper;
}
```