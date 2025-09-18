# executeBinaryArithmExpr

## Location
src/backend/utils/adt/jsonpath_exec.c: 2105 - 2175

## Overview
Executes binary arithmetic expressions on singleton numeric operands with automatic array unwrapping in lax mode, supporting standard arithmetic operations in JSON path expressions.

## Definition
```c
static JsonPathExecResult executeBinaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, BinaryArithmFunc func, JsonValueList *found)
```

## Detailed Description
This function implements binary arithmetic operations for JSON path expressions by:

1. **Operand Extraction**: Uses jspGetLeftArg and jspGetRightArg to extract left and right operands
2. **Sequence Evaluation**: Evaluates both operands with auto-unwrapping enabled to handle array operands in lax mode  
3. **Singleton Validation**: Ensures both operands resolve to exactly one numeric value, generating appropriate error messages if not
4. **Arithmetic Computation**: Applies the provided BinaryArithmFunc callback to perform the actual arithmetic operation
5. **Error Handling**: Supports both throwing and non-throwing error modes based on context settings
6. **Result Chaining**: If there are subsequent operations, creates a new JsonbValue with the result and continues execution

The function extends the standard behavior by unwrapping array operands for all binary arithmetic expressions, not just multiplicative ones as specified in the standard. This provides more intuitive behavior in JSON path arithmetic operations.

## Parameters / Member Variables
- `cxt`: Pointer to JSON path execution context containing mode settings and error handling preferences
- `jsp`: Pointer to the JSON path item representing the binary arithmetic operation
- `jb`: Pointer to current JsonbValue context for operand evaluation
- `func`: Function pointer to the specific arithmetic operation to perform (add, subtract, multiply, divide, modulo)
- `found`: Pointer to JsonValueList for collecting results (NULL for intermediate operations)

## Dependencies
- Functions called/Symbols referenced:
  - jspGetLeftArg
  - jspGetRightArg
  - executeItemOptUnwrapResult
  - JsonValueListLength
  - JsonValueListHead
  - getScalar
  - jspOperationName
  - jspThrowErrors
  - jspGetNext
  - executeNextItem
  - jperIsError
- Called from (representative examples):
  - executeItemOptUnwrapTarget (for addition, subtraction, multiplication, division, modulo operations)

## Notes and Other Information
- This is a static function used only within the jsonpath_exec.c compilation unit
- Extends JSON path standard by unwrapping arrays for all binary arithmetic operations, not just multiplicative
- Requires both operands to be singleton numeric values - generates specific error messages for violations
- Supports PostgreSQL's Numeric type for precise decimal arithmetic
- Error handling mode affects whether arithmetic errors are thrown as exceptions or returned as jperError
- Part of the comprehensive JSON path arithmetic expression evaluation system
- The function creates new JsonbValue results on the heap when continuing execution chains