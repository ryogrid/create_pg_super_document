# defGetNumeric

## Location
src/backend/commands/define.c: 81 - 106

## Overview
Extracts a numeric value (as a double) from a DefElem, converting integer and float nodes to double precision floating-point values.

## Definition
```c
double defGetNumeric(DefElem *def)
```

## Detailed Description
The `defGetNumeric` function extracts numeric values from DefElem nodes and returns them as double-precision floating-point numbers. It specifically handles T_Integer and T_Float node types, converting them appropriately to double values. This function is commonly used in PostgreSQL command processing where numeric parameters need to be extracted from SQL definition elements.

Unlike `defGetString` which handles multiple node types, `defGetNumeric` is more restrictive and only accepts numeric node types (integers and floats). If a non-numeric node type is encountered or if the DefElem has no argument, the function reports a syntax error.

## Parameters / Member Variables
- `def`: A pointer to a DefElem structure containing the definition element from which to extract a numeric value

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type)
  - nodeTag (for type checking)
  - intVal (to extract integer values and cast to double)
  - floatVal (to extract float values)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md)/errmsg (for error handling)
  
- Called from (representative examples):
  - [compute_function_attributes](../c/compute_function_attributes.md) (src/backend/commands/functioncmds.c:820)
  - [compute_function_attributes](../c/compute_function_attributes.md) (src/backend/commands/functioncmds.c:828)
  - [AlterFunction](../A/AlterFunction.md) (src/backend/commands/functioncmds.c:1423)
  - [AlterFunction](../A/AlterFunction.md) (src/backend/commands/functioncmds.c:1431)

## Notes and Other Information
- Only accepts T_Integer and T_Float node types, unlike the more flexible `defGetString`
- Always returns a double-precision floating-point value
- Throws a syntax error for non-numeric node types or missing arguments
- The function is located in src/backend/commands/define.c:81-106  
- Primarily used in function definition processing where numeric attributes are specified
- Integer values are automatically cast to double precision