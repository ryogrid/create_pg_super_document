# pg_input_is_valid_common

## Location
[src/backend/utils/adt/misc.c:765-827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L765-L827)

## Overview
A static utility function that validates whether a given text string can be successfully parsed as a specified PostgreSQL data type.

## Definition

```c
static bool
pg_input_is_valid_common(FunctionCallInfo fcinfo,
						 text *txt, text *typname,
						 ErrorSaveContext *escontext)
```
## Detailed Description
This function serves as the common implementation for PostgreSQL input validation functions. It attempts to parse a text string using the input function of a specified data type, returning true if the conversion succeeds or false if it fails. The function implements an optimization by caching type information across multiple calls to avoid repeated lookups when the same data type is used consecutively.

The function uses a ValidIOData structure stored in fcinfo->flinfo->fn_extra to cache:
- Type OID and type modifier information
- Input function details (OID, parameter, and FmgrInfo)
- Whether the typename parameter is constant (for optimization)

## Parameters / Member Variables
- : Function call information structure containing metadata about the function call
- : Text string to be validated for the specified data type
- : Text representation of the PostgreSQL data type name to validate against
- : Error save context for controlled error handling, allowing the function to return false instead of throwing exceptions

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [get_fn_expr_arg_stable](../g/get_fn_expr_arg_stable.md)
  - [parseTypeString](parseTypeString.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
- Called from (representative examples):
  - [pg_input_is_valid](pg_input_is_valid.md)
  - [pg_input_error_info](pg_input_error_info.md)

## Notes and Other Information
- This is a static function used internally within misc.c
- Implements performance optimization by caching type information between calls
- Uses ErrorSaveContext to handle conversion errors gracefully without throwing exceptions
- The function is designed to be reusable across different input validation scenarios
- Memory allocation for caching occurs in the function's memory context to ensure proper cleanup