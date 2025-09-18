# InputFunctionCallSafe

## Location
src/backend/utils/fmgr/fmgr.c: 1585 - 1639

## Overview
InputFunctionCallSafe is a safe variant of InputFunctionCall that provides non-exception handling of "soft" errors during datatype input function calls.

## Definition
```c
bool InputFunctionCallSafe(FmgrInfo *flinfo, char *str, Oid typioparam, int32 typmod, fmNodePtr escontext, Datum *result)
```

## Detailed Description
This function serves as a safer alternative to InputFunctionCall by providing error handling capabilities that don't throw exceptions for "soft" errors. Instead of directly returning the converted Datum, it returns a boolean success indicator while storing the result in an output parameter. When an ErrorSaveContext is provided, soft errors are captured and reported through the context structure rather than throwing exceptions. This allows callers to handle conversion failures gracefully without disrupting normal program flow through exception handling.

## Parameters / Member Variables
- `flinfo`: Function manager info structure containing details about the input function to call
- `str`: String representation of the value to convert (may be NULL to indicate a NULL value)
- `typioparam`: OID parameter passed to the input function (type-specific parameter)
- `typmod`: Type modifier value providing additional type information
- `escontext`: Error save context node for capturing soft errors (fmNodePtr type)
- `result`: Output parameter where the converted Datum is stored

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - InitFunctionCallInfoData (initializes function call structure)
  - CStringGetDatum (converts C string to Datum)
  - FunctionCallInvoke (invokes the actual function)
  - SOFT_ERROR_OCCURRED (checks if a soft error occurred)
- Called from (representative examples):
  - NextCopyFrom
  - ReadArrayStr
  - domain_in
  - populate_scalar
  - pg_input_is_valid_common

## Notes and Other Information
- Returns true for success, false for failure, with the actual result stored in the result parameter
- Supports soft error handling through ErrorSaveContext - errors don't throw exceptions but are captured
- If no ErrorSaveContext is provided, behaves functionally identical to InputFunctionCall
- Particularly useful in COPY operations and validation scenarios where input errors should not abort the entire operation
- Part of PostgreSQL's enhanced error handling infrastructure for graceful degradation