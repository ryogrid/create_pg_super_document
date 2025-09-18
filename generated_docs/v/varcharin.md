# varcharin

## Location
src/backend/utils/adt/varchar.c: 495 - 515

## Overview
Input function for the varchar data type that converts C string representation to PostgreSQL's internal varchar format with proper length validation.

## Definition
```c
Datum varcharin(PG_FUNCTION_ARGS)
```

## Detailed Description
The `varcharin` function serves as the primary input function for PostgreSQL's varchar data type. It takes a null-terminated C string and converts it to the internal VARCHAR representation, applying length constraints specified by the type modifier. The function delegates the core processing logic to `varchar_input`, which handles multibyte character processing, length validation, and proper truncation behavior according to SQL standards. It automatically determines the string length and passes the function call context for error handling.

## Parameters / Member Variables
- Takes input through `PG_FUNCTION_ARGS` macro which provides:
  - `s`: A C string (char *) containing the input text to be converted
  - `typelem`: Element type OID (unused, marked with NOT_USED)
  - `atttypmod`: Type modifier specifying maximum length plus header size

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING`: Extracts C string argument from function call context
  - `PG_GETARG_INT32`: Extracts type modifier from function call context
  - [varchar_input](varchar_input.md): Core function that performs length validation and conversion
  - `strlen`: Standard C function to determine string length
  - `PG_RETURN_VARCHAR_P`: Returns VarChar pointer as Datum
- Called from (representative examples):
  - No direct callers found in the codebase (typically called by PostgreSQL's type system during input conversion)

## Notes and Other Information
- Located in `src/backend/utils/adt/varchar.c:495-515`
- Part of PostgreSQL's type input/output system, automatically invoked during data conversion
- Relies on `varchar_input` for the actual processing logic
- The `typelem` parameter is marked as NOT_USED, indicating it's not needed for varchar processing
- Uses `fcinfo->context` to pass error context for soft error handling
- This function is registered as the input function for varchar in PostgreSQL's type system catalog