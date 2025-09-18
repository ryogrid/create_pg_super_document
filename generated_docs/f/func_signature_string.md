# func_signature_string

## Location
src/backend/parser/parse_func.c: 2030 - 2048

## Overview
A convenience wrapper around  that accepts a qualified function name as a list rather than a string.

## Definition


## Detailed Description
The  function serves as a simple wrapper around , providing the same functionality but accepting the function name as a qualified name list (e.g., ) instead of a pre-formatted string. This is particularly useful when working with qualified function names that need to be converted to their string representation for display in error messages or logging.

The function internally converts the qualified name list to a string using  and then delegates to  to perform the actual signature formatting.

## Parameters / Member Variables
- : List of strings representing the qualified function name (e.g., schema.function_name)
- : Total number of arguments in the function signature
- : List of C strings containing the names for the last N arguments (can be NIL if no named args)
- : Array of OIDs representing the types of each argument

## Dependencies
- Functions called/Symbols referenced:
  - NameListToString
  - funcname_signature_string
- Called from (representative examples):
  - ParseFuncOrColumn
  - LookupFuncName
  - LookupFuncWithArgs
  - lookup_agg_function
  - findTypeInputFunction
  - findTypeOutputFunction
  - Various type-related functions in typecmds.c

## Notes and Other Information
- This is essentially a convenience function that eliminates the need for callers to manually convert qualified name lists to strings
- Widely used throughout the PostgreSQL codebase for error message generation
- Returns a palloc'd string that should be freed by the caller when no longer needed
- The most commonly used function signature formatting function in PostgreSQL, as many parts of the system work with qualified name lists rather than pre-formatted strings
- Supports the same named argument formatting as its underlying  function