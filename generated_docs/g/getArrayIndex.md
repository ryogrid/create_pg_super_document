# getArrayIndex

## Location
[src/backend/utils/adt/jsonpath_exec.c:3459-3493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3459-L3493)

## Overview
Executes array subscript expression and converts the resulting numeric item to an integer type with truncation for JSON array indexing operations.

## Definition
```c
static JsonPathExecResult getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, int32 *index)
```

## Detailed Description
The getArrayIndex function is responsible for evaluating array subscript expressions in JSONPath operations and converting the result to a valid integer index. It executes the given JSONPath item expression, validates that the result is a single numeric value, truncates it to an integer, and ensures it falls within the valid integer range. This function is crucial for array access operations in JSONPath expressions.

## Parameters / Member Variables
- `cxt`: JSONPath execution context containing state and configuration
- `jsp`: JSONPath item representing the subscript expression to evaluate
- `jb`: JsonbValue containing the JSON data being operated on
- `index`: Output parameter where the calculated integer index will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [executeItem](../e/executeItem.md) (executes JSONPath expressions)
  - jperIsError (checks for execution errors)
  - [JsonValueListLength](../J/JsonValueListLength.md) (gets list length)
  - [JsonValueListHead](../J/JsonValueListHead.md) (gets first list item)
  - [getScalar](getScalar.md) (extracts scalar values)
  - DirectFunctionCall2 (PostgreSQL function call interface)
  - [numeric_trunc](../n/numeric_trunc.md) (truncates numeric values)
  - [numeric_int4_opt_error](../n/numeric_int4_opt_error.md) (converts numeric to int32 with error checking)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (array access operations)

## Notes and Other Information
- Validates that the subscript expression evaluates to exactly one numeric value
- Truncates the numeric result to remove any fractional part
- Performs range checking to ensure the result fits in a 32-bit signed integer
- Returns appropriate error codes for invalid subscripts (non-numeric, multiple values, out of range)
- Uses PostgreSQL's numeric type system for precise arithmetic operations
- Part of the JSONPath execution engine for handling array indexing operations