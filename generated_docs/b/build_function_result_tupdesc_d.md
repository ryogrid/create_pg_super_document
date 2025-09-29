# build_function_result_tupdesc_d

## Location
[src/backend/utils/fmgr/funcapi.c:1751-1869](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1751-L1869)

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
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [palloc](../p/palloc.md)
  - TextDatumGetCString
  - [psprintf](../p/psprintf.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - PROARGMODE_IN, PROARGMODE_VARIADIC, PROARGMODE_OUT, PROARGMODE_INOUT, PROARGMODE_TABLE
  - PROKIND_PROCEDURE
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [build_function_result_tupdesc_t](build_function_result_tupdesc_t.md)
  - TypeFuncClass

## Notes and Other Information
- Returns NULL if input arrays are NULL or if functions have fewer than 2 output arguments
- Procedures can have 0 or 1 output arguments and still return a valid descriptor
- Generates default column names ("column1", "column2", etc.) for unnamed parameters
- Validates array structure and dimensions, throwing errors for malformed input
- Creates a template tuple descriptor and initializes each entry with proper type and name information
- Used during function creation to pre-compute result types before catalog insertion

## Simplified Source

```c
TupleDesc build_function_result_tupdesc_d(char prokind,
                                          Datum proallargtypes,
                                          Datum proargmodes,
                                          Datum proargnames)
{
    TupleDesc desc;
    ArrayType *arr;
    int numargs;
    Oid *argtypes;
    char *argmodes;
    Datum *argnames = NULL;
    Oid *outargtypes;
    char **outargnames;
    int numoutargs;
    int nargnames;
    int i;

    // Return NULL if required arrays are missing
    if (proallargtypes == PointerGetDatum(NULL) ||
        proargmodes == PointerGetDatum(NULL))
        return NULL;

    // Extract and validate argument types array
    arr = DatumGetArrayTypeP(proallargtypes);
    numargs = ARR_DIMS(arr)[0];
    if (ARR_NDIM(arr) != 1 || numargs < 0 || ARR_HASNULL(arr) ||
        ARR_ELEMTYPE(arr) != OIDOID)
        elog(ERROR, "proallargtypes is not a 1-D Oid array or it contains nulls");
    argtypes = (Oid *) ARR_DATA_PTR(arr);

    // Extract and validate argument modes array
    arr = DatumGetArrayTypeP(proargmodes);
    if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs ||
        ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID)
        elog(ERROR, "proargmodes is not a 1-D char array of length %d or it contains nulls", numargs);
    argmodes = (char *) ARR_DATA_PTR(arr);

    // Extract argument names if provided
    if (proargnames != PointerGetDatum(NULL)) {
        arr = DatumGetArrayTypeP(proargnames);
        if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs ||
            ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
            elog(ERROR, "proargnames is not a 1-D text array of length %d or it contains nulls", numargs);
        deconstruct_array_builtin(arr, TEXTOID, &argnames, NULL, &nargnames);
        Assert(nargnames == numargs);
    }

    // Handle empty argument list
    if (numargs <= 0)
        return NULL;

    // Extract output arguments (OUT, INOUT, TABLE modes)
    outargtypes = (Oid *) palloc(numargs * sizeof(Oid));
    outargnames = (char **) palloc(numargs * sizeof(char *));
    numoutargs = 0;

    for (i = 0; i < numargs; i++) {
        char *pname;

        // Skip input-only and variadic arguments
        if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC)
            continue;

        Assert(argmodes[i] == PROARGMODE_OUT ||
               argmodes[i] == PROARGMODE_INOUT ||
               argmodes[i] == PROARGMODE_TABLE);

        outargtypes[numoutargs] = argtypes[i];

        // Get parameter name or generate default
        if (argnames)
            pname = TextDatumGetCString(argnames[i]);
        else
            pname = NULL;

        if (pname == NULL || pname[0] == '\0') {
            pname = psprintf("column%d", numoutargs + 1);
        }
        outargnames[numoutargs] = pname;
        numoutargs++;
    }

    // Functions need at least 2 output args to return tuples
    if (numoutargs < 2 && prokind != PROKIND_PROCEDURE)
        return NULL;

    // Create and populate tuple descriptor
    desc = CreateTemplateTupleDesc(numoutargs);
    for (i = 0; i < numoutargs; i++) {
        TupleDescInitEntry(desc, i + 1,
                          outargnames[i],
                          outargtypes[i],
                          -1, 0);
    }

    return desc;
}
```