# build_function_result_tupdesc_d

## Location
src/backend/utils/fmgr/funcapi.c: 1751 - 1869

## Overview
Constructs a tuple descriptor for RECORD functions from argument type, mode, and name arrays, handling the core logic for building result rowtypes from function metadata.

## Definition
```c
TupleDesc build_function_result_tupdesc_d(char prokind, Datum proallargtypes, Datum proargmodes, Datum proargnames)
```

## Detailed Description
This function builds a TupleDesc (tuple descriptor) for functions that return RECORD types based on their output parameters. It processes the function's argument arrays to identify output parameters (OUT, INOUT, TABLE modes) and constructs a corresponding tuple descriptor with appropriate column names and types.

The function validates the input arrays for proper structure, extracts output argument information, generates column names (using provided names or default "columnN" names), and creates a tuple descriptor. For functions (not procedures), it requires at least two output arguments to return a valid tuple descriptor.

This function is split out from build_function_result_tupdesc_t to allow ProcedureCreate to compute tuple descriptors before actually creating the function in the catalog.

## Parameters / Member Variables
- `prokind`: The function kind (PROKIND_FUNCTION, PROKIND_PROCEDURE, etc.) to determine output argument requirements
- `proallargtypes`: Datum containing array of all argument types (OID array)
- `proargmodes`: Datum containing array of argument modes (char array with IN/OUT/INOUT/TABLE/VARIADIC)
- `proargnames`: Datum containing array of argument names (text array, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - deconstruct_array_builtin
  - palloc
  - TextDatumGetCString
  - psprintf
  - CreateTemplateTupleDesc
  - TupleDescInitEntry
  - PROARGMODE_IN, PROARGMODE_VARIADIC, PROARGMODE_OUT, PROARGMODE_INOUT, PROARGMODE_TABLE
  - PROKIND_PROCEDURE
- Called from (representative examples):
  - ProcedureCreate
  - build_function_result_tupdesc_t
  - TypeFuncClass

## Notes and Other Information
- Returns NULL if input arrays are NULL or if functions have fewer than 2 output arguments
- Procedures can have 0 or 1 output arguments and still return a valid descriptor
- Generates default column names ("column1", "column2", etc.) for unnamed parameters
- Validates array structure and dimensions, throwing errors for malformed input
- Creates a template tuple descriptor and initializes each entry with proper type and name information
- Used during function creation to pre-compute result types before catalog insertion