# bpcharin

## Location
[src/backend/utils/adt/varchar.c:198-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L198-L218)

## Overview
A PostgreSQL input function that converts C string representation to internal BPCHAR (fixed-length character) type, serving as the entry point for text input processing.

## Definition
```c
Datum bpcharin(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the standard PostgreSQL input function for the BPCHAR (CHAR(n)) data type. It follows the PostgreSQL function manager (fmgr) calling convention and is typically invoked when converting string literals or text input into the internal BPCHAR representation. The function extracts the input string and type modifier from the function arguments, delegates the actual processing to bpchar_input(), and returns the result in the appropriate PostgreSQL Datum format. It supports both normal error handling and soft error contexts for better error recovery.

## Parameters / Member Variables
- Function follows PG_FUNCTION_ARGS convention with these arguments accessible via macros:
  - Argument 0: C string input (accessed via PG_GETARG_CSTRING(0))
  - Argument 1: Type element OID (currently unused)
  - Argument 2: Attribute type modifier (accessed via PG_GETARG_INT32(2))

## Dependencies
- Functions called/Symbols referenced:
  - [bpchar_input](bpchar_input.md) (core processing logic)
  - PG_GETARG_CSTRING (argument extraction macro)
  - PG_GETARG_INT32 (argument extraction macro)  
  - PG_RETURN_BPCHAR_P (return value macro)
  - strlen (string length calculation)
- Called from (representative examples):
  - PostgreSQL type system during input conversion
  - SQL parsing and execution engine

## Notes and Other Information
- This is a public PostgreSQL function registered in the system catalogs for BPCHAR type
- The second argument (typelem OID) is marked as NOT_USED but kept for compatibility
- Uses fcinfo->context to pass error handling context to bpchar_input
- Returns a Datum that contains a pointer to the created BpChar structure
- Automatically handles memory management through PostgreSQL's memory context system

## Simplified Source

```c
Datum bpcharin(PG_FUNCTION_ARGS) {
    // Extract function arguments
    char *input_string = PG_GETARG_CSTRING(0);
    // Argument 1 (typelem) is unused
    int32 atttypmod = PG_GETARG_INT32(2);

    // Process the input string using common bpchar logic
    BpChar *result = bpchar_input(input_string, strlen(input_string),
                                  atttypmod, fcinfo->context);

    // Return the processed result
    PG_RETURN_BPCHAR_P(result);
}
```