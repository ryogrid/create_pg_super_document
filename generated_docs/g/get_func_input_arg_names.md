# get_func_input_arg_names

## Location
[src/backend/utils/fmgr/funcapi.c:1522-1606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1522-L1606)

## Overview
Extracts the names of only input arguments from a function's proargnames and proargmodes arrays, filtering out output and table parameters.

## Definition

```c
int
get_func_input_arg_names(Datum proargnames, Datum proargmodes,
                         char ***arg_names)
```
## Detailed Description
This function processes the proargnames and proargmodes arrays from a function's pg_proc entry to extract only the names of input arguments (IN, INOUT, and VARIADIC modes). It filters out output arguments (OUT and TABLE modes) to provide a focused view of the function's input interface.

The function handles cases where argument names may be missing (represented as empty strings) by setting those entries to NULL in the output array. It also gracefully handles functions without argument names or modes by returning appropriate default values.

The function validates both input arrays for proper structure and dimensions, ensuring they are well-formed 1-D arrays of the correct types.

## Parameters / Member Variables
- `proargnames`: Datum containing the proargnames array from pg_proc (or PointerGetDatum(NULL) if none)
- `proargmodes`: Datum containing the proargmodes array from pg_proc (or PointerGetDatum(NULL) if none)
- `***arg_names`: Output parameter receiving palloc'd array of input argument name strings
## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DIMS, ARR_DATA_PTR
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - PROARGMODE_IN, PROARGMODE_INOUT, PROARGMODE_VARIADIC
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md) (twice)
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md)
  - TypeFuncClass (referenced in funcapi.h)

## Notes and Other Information
- Returns the number of input arguments (excluding OUT and TABLE parameters)
- Returns 0 and sets arg_names to NULL if proargnames is NULL or no input arguments exist
- Handles unnamed arguments by setting corresponding array entries to NULL
- Validates array structure and reports errors for malformed data
- Essential for distinguishing input parameters from output parameters in function signatures
- Used by function creation and SQL function preparation routines that need to focus on input parameters only
- Part of PostgreSQL's function introspection system for analyzing function interfaces

## Simplified Source

```c
int get_func_input_arg_names(Datum proargnames, Datum proargmodes,
                             char ***arg_names) {
    ArrayType *arr;
    int numargs;
    Datum *argnames;
    char *argmodes;
    char **inargnames;
    int numinargs;
    int i;

    // Handle NULL proargnames
    if (proargnames == PointerGetDatum(NULL)) {
        *arg_names = NULL;
        return 0;
    }

    // Extract and validate proargnames array
    arr = DatumGetArrayTypeP(proargnames);
    if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
        elog(ERROR, "proargnames is not a 1-D text array or it contains nulls");
    deconstruct_array_builtin(arr, TEXTOID, &argnames, NULL, &numargs);

    // Extract and validate proargmodes array if present
    if (proargmodes != PointerGetDatum(NULL)) {
        arr = DatumGetArrayTypeP(proargmodes);
        if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs ||
            ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID)
            elog(ERROR, "proargmodes is not a 1-D char array of length %d", numargs);
        argmodes = (char *) ARR_DATA_PTR(arr);
    } else {
        argmodes = NULL;
    }

    // Handle empty arrays
    if (numargs <= 0) {
        *arg_names = NULL;
        return 0;
    }

    // Extract names for input arguments only
    inargnames = (char **) palloc(numargs * sizeof(char *));
    numinargs = 0;
    for (i = 0; i < numargs; i++) {
        // Check if this is an input argument
        if (argmodes == NULL ||
            argmodes[i] == PROARGMODE_IN ||
            argmodes[i] == PROARGMODE_INOUT ||
            argmodes[i] == PROARGMODE_VARIADIC) {

            char *pname = TextDatumGetCString(argnames[i]);

            // Store name or NULL if empty
            if (pname[0] != '\0')
                inargnames[numinargs] = pname;
            else
                inargnames[numinargs] = NULL;
            numinargs++;
        }
    }

    *arg_names = inargnames;
    return numinargs;
}
```