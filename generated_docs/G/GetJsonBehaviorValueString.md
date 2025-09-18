# GetJsonBehaviorValueString

## Location
src/backend/executor/execExprInterp.c: 4608 - 4635

## Overview
Returns a human-readable string representation of JSON behavior types for error reporting and debugging purposes.

## Definition
```c
static char *GetJsonBehaviorValueString(JsonBehavior *behavior)
```

## Detailed Description
This static utility function provides string representations of JsonBehaviorType enumeration values, primarily used for error message formatting in JSON expression error handling. It maintains a static array of behavior names that directly corresponds to the JsonBehaviorType enumeration order, ensuring consistent and readable error messages when JSON ON ERROR or ON EMPTY behaviors are referenced in error reporting. The function returns a duplicated string to ensure memory safety in error contexts.

## Parameters / Member Variables
- `behavior`: JsonBehavior pointer containing the behavior type to convert
  - `behavior->btype`: JsonBehaviorType enumeration value to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - pstrdup
- Called from (representative examples):
  - ExecEvalJsonCoercionFinish (for error message formatting)

## Notes and Other Information
- Returns one of: "NULL", "ERROR", "EMPTY", "TRUE", "FALSE", "UNKNOWN", "EMPTY ARRAY", "EMPTY OBJECT", "DEFAULT"
- The behavior_names array order must match JsonBehaviorType enumeration order
- Uses pstrdup to return a safely allocated copy of the string
- Static function scope limits its use to the execExprInterp.c compilation unit
- Primarily used in error message construction when JSON behavior expressions fail
- Essential for providing meaningful error messages in SQL/JSON operations
- Used when coercion errors occur in ON ERROR or ON EMPTY behavior evaluation
- Supports debugging and user feedback for JSON expression error handling
- The string representations match SQL/JSON standard behavior clause syntax