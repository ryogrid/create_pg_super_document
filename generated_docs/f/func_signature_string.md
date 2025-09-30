# func_signature_string

## Location
[src/backend/parser/parse_func.c:2030-2048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L2030-L2048)

## Overview
A convenience wrapper around  that accepts a qualified function name as a list rather than a string.

## Definition

```c
const char *
func_signature_string(List *funcname, int nargs,
					  List *argnames, const Oid *argtypes)
```
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
  - [NameListToString](../N/NameListToString.md)
  - [funcname_signature_string](funcname_signature_string.md)
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [LookupFuncName](../L/LookupFuncName.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [lookup_agg_function](../l/lookup_agg_function.md)
  - [findTypeInputFunction](findTypeInputFunction.md)
  - [findTypeOutputFunction](findTypeOutputFunction.md)
  - Various type-related functions in typecmds.c

## Notes and Other Information
- This is essentially a convenience function that eliminates the need for callers to manually convert qualified name lists to strings
- Widely used throughout the PostgreSQL codebase for error message generation
- Returns a palloc'd string that should be freed by the caller when no longer needed
- The most commonly used function signature formatting function in PostgreSQL, as many parts of the system work with qualified name lists rather than pre-formatted strings
- Supports the same named argument formatting as its underlying  function

## Simplified Source

```c
const char *
func_signature_string(List *funcname, int nargs,
                      List *argnames, const Oid *argtypes)
{
    // Convert qualified name list to string and delegate to core function
    return funcname_signature_string(NameListToString(funcname),
                                     nargs, argnames, argtypes);
}
```