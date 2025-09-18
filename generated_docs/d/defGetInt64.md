# defGetInt64

## Location
src/backend/commands/define.c: 186 - 218

## Overview
Extracts a 64-bit signed integer value from a DefElem, accepting both T_Integer and T_Float node types, with special handling for large numeric values.

## Definition
```c
int64 defGetInt64(DefElem *def)
```

## Detailed Description
The `defGetInt64` function extracts integer values from DefElem nodes and returns them as 64-bit signed integers (int64). It is more sophisticated than `defGetInt32` as it can handle both T_Integer and T_Float node types. The function has special logic to handle large numeric values that exceed the range of 32-bit integers.

When the PostgreSQL lexer encounters integer values too large for int4 (32-bit integers), it represents them as Float constants. The `defGetInt64` function recognizes this pattern and converts such Float values back to int64 using PostgreSQL's int8in function, ensuring that large integer values are properly handled.

The function is specifically designed for cases where 64-bit integer precision is required, such as sequence parameters that may need to handle very large numbers.

## Parameters / Member Variables
- `def`: A pointer to a DefElem structure containing the definition element from which to extract a 64-bit integer value

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type)
  - nodeTag (for type checking)
  - intVal (to extract integer values and cast to int64)
  - [DatumGetInt64](../D/DatumGetInt64.md) (to convert Datum to int64)
  - DirectFunctionCall1 (to call PostgreSQL functions)
  - [int8in](../i/int8in.md) (PostgreSQL function to parse int8 from string)
  - [CStringGetDatum](../C/CStringGetDatum.md) (to convert C string to Datum)
  - Float (cast node type)
  - castNode (for safe type casting)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md)/errmsg (for error handling)
  
- Called from (representative examples):
  - [parse_basebackup_options](../p/parse_basebackup_options.md) (src/backend/backup/basebackup.c:805)
  - [init_params](../i/init_params.md) (src/backend/commands/sequence.c:1414)
  - [init_params](../i/init_params.md) (src/backend/commands/sequence.c:1441)
  - [init_params](../i/init_params.md) (src/backend/commands/sequence.c:1473)
  - [init_params](../i/init_params.md) (src/backend/commands/sequence.c:1513)
  - [init_params](../i/init_params.md) (src/backend/commands/sequence.c:1541)
  - [init_params](../i/init_params.md) (src/backend/commands/sequence.c:1570)

## Notes and Other Information
- Handles both T_Integer and T_Float node types, unlike the more restrictive `defGetInt32`
- Special logic to handle large integers that the lexer represents as Float constants
- Uses PostgreSQL's int8in function via DirectFunctionCall1 to parse large numeric string values
- Returns int64 type, suitable for PostgreSQL's 64-bit integer requirements  
- The function is located in src/backend/commands/define.c:186-218
- Primarily used in sequence operations and other contexts requiring 64-bit integer precision
- More complex than other defGet functions due to the Float-to-int64 conversion logic
- Requires an explicit argument value; reports error if def->arg is NULL