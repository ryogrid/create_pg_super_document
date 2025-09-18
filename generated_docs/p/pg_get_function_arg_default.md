# pg_get_function_arg_default

## Location
src/backend/utils/adt/ruleutils.c: 3440 - 3509

## Overview
A PostgreSQL SQL function that returns the textual representation of a function argument's default value for a specific argument position.

## Definition
```c
Datum pg_get_function_arg_default(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves and formats the default value of a function argument based on the function OID and argument position. It takes a function ID and an argument number (1-based indexing among all arguments, including OUT parameters) and returns the SQL representation of that argument's default value. The function handles the complex logic of mapping argument positions to default value positions, since default values only apply to input arguments and are stored in the order of the last N input arguments where N is the number of arguments with defaults.

## Parameters / Member Variables
- `funcid` (PG_GETARG_OID(0)): The OID of the function to examine
- `nth_arg` (PG_GETARG_INT32(1)): The 1-based argument position among all arguments (proallargtypes)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - get_func_arg_info
  - is_input_argument (called twice)
  - SysCacheGetAttr
  - TextDatumGetCString
  - stringToNode
  - list_nth
  - deparse_expression
  - string_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- Uses 1-based indexing for argument positions to match information_schema.sql conventions
- Only works with input arguments (IN, INOUT, VARIADIC modes)
- Returns NULL if the function doesn't exist, argument position is invalid, or no default value exists
- The function performs complex index calculations since proargdefaults only stores the last N input arguments that have defaults
- Default values are stored as serialized Node structures and are deserialized and deparsed back to SQL text
- This function is typically exposed to SQL as pg_get_function_arg_default(funcid, argnum)