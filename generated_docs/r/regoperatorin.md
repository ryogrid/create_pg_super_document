# regoperatorin

## Location
src/backend/utils/adt/regproc.c: 639 - 693

## Overview
Converts operator name with argument types in the format "oprname(args)" to operator OID, used for regoperator data type input.

## Definition
```c
Datum regoperatorin(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regoperatorin` function is the input function for PostgreSQL's regoperator data type. It converts a string representation of an operator (including its argument types) into the corresponding operator OID. Unlike `regoperin` which handles operators by name only, this function requires explicit argument type specification to resolve operator overloading.

The function accepts several input formats:
1. Numeric OID (e.g., "123")
2. Special value "0" for invalid/unknown operators
3. Operator with argument types (e.g., "+(integer,integer)")

The function performs comprehensive validation:
- Parses numeric OIDs directly
- In bootstrap mode, only accepts numeric OIDs
- For operator names, parses the name and argument types
- Validates exactly 2 arguments are provided (operators must be binary)
- Looks up the operator in the system catalog using name and argument types
- Returns appropriate errors for missing or invalid operators

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: C string containing operator name with argument types or numeric OID
  - `fcinfo->context`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING`: Extract C string argument
  - `parseNumericOid`: Parse numeric OID from string
  - `PG_RETURN_OID`: Return OID result
  - `IsBootstrapProcessingMode`: Check if in bootstrap mode
  - `elog`: Log error message
  - `parseNameAndArgTypes`: Parse operator name and argument types
  - `PG_RETURN_NULL`: Return NULL result
  - `ereturn`: Return with error context
  - `errcode`: Set error code
  - `errmsg`: Set error message
  - `errhint`: Set error hint
  - `OpernameGetOprid`: Get operator OID by name and argument types
  - `OidIsValid`: Check if OID is valid
  - `FUNC_MAX_ARGS`: Maximum function arguments constant

- Called from (representative examples):
  - `to_regoperator`: Safe version that returns NULL on error (src/backend/utils/adt/regproc.c:700)

## Notes and Other Information
- This function differs from `regoperin` by requiring explicit argument types to handle operator overloading
- Supports only binary operators (exactly 2 arguments)
- Provides helpful error hints for common mistakes (missing arguments, too many arguments)
- Part of PostgreSQL's regtype family for type/object name resolution
- The function is stricter than `regoperin` because operator overloading requires precise type matching
- Used primarily for the regoperator data type which stores operators with their complete signatures