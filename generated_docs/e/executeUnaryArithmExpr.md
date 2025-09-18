# executeUnaryArithmExpr

## Location
src/backend/utils/adt/jsonpath_exec.c: 2176 - 2242

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
  - jspGetArg
  - executeItemOptUnwrapResult
  - jspGetNext
  - JsonValueListInitIterator
  - JsonValueListNext
  - getScalar
  - jspOperationName
  - DirectFunctionCall1
  - DatumGetNumeric
  - NumericGetDatum
  - executeNextItem
  - jperIsError
- Called from (representative examples):
  - executeItemOptUnwrapTarget (for unary plus and minus operations)

## Notes and Other Information
- This is a static function used only within the jsonpath_exec.c compilation unit
- Processes sequences of values rather than requiring singleton operands like binary arithmetic
- Uses PostgreSQL's function call interface (DirectFunctionCall1) for arithmetic operations
- Handles the identity operation when func is NULL (unary plus that doesn't modify the value)
- Non-numeric values are silently skipped in some contexts but cause errors when results are required
- Part of the JSON path arithmetic expression system supporting unary operators
- Automatic array unwrapping in lax mode enables intuitive arithmetic on array elements
- Each numeric value is processed independently, allowing for vectorized-style operations on sequences