# DirectInputFunctionCallSafe

## Location
[src/backend/utils/fmgr/fmgr.c:1640-1682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1640-L1682)

## Overview
DirectInputFunctionCallSafe is a variant of InputFunctionCallSafe that calls a directly-named C function pointer rather than using the function manager lookup system.

## Definition
```c
bool DirectInputFunctionCallSafe(PGFunction func, char *str, Oid typioparam, int32 typmod, fmNodePtr escontext, Datum *result)
```

## Detailed Description
This function provides safe input function calling when you have a direct pointer to the C function rather than going through PostgreSQL's function manager system. It assumes the target function is strict (NULL input produces NULL output) and that the function doesn't need to examine FmgrInfo structure since none is provided. Like InputFunctionCallSafe, it supports soft error handling through ErrorSaveContext and returns success/failure status while storing the converted result in an output parameter.

## Parameters / Member Variables
- `func`: Direct pointer to the C function to call (PGFunction type)
- `str`: String representation of the value to convert (may be NULL to indicate a NULL value)
- `typioparam`: OID parameter passed to the input function (type-specific parameter)
- `typmod`: Type modifier value providing additional type information
- `escontext`: Error save context node for capturing soft errors (fmNodePtr type)
- `result`: Output parameter where the converted Datum is stored

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - InitFunctionCallInfoData (initializes function call structure with NULL FmgrInfo)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to Datum)
  - SOFT_ERROR_OCCURRED (checks if a soft error occurred)
- Called from (representative examples):
  - [jsonb_in_scalar](../j/jsonb_in_scalar.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [to_regproc](../t/to_regproc.md)
  - [to_regprocedure](../t/to_regprocedure.md)
  - [to_regoper](../t/to_regoper.md)
  - [parseNumericOid](../p/parseNumericOid.md)

## Notes and Other Information
- More efficient than InputFunctionCallSafe when you already have the function pointer
- Assumes target function is strict - automatically returns NULL for NULL input
- Cannot be used with functions that need to access FmgrInfo structure
- Primarily used for built-in PostgreSQL data types where the C function is known at compile time
- Used extensively in reg* type functions (regproc, regclass, etc.) and JSON processing
- Part of PostgreSQL's optimized path for direct function calls without catalog lookups