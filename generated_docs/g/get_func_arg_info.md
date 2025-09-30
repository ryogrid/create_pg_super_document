# get_func_arg_info

## Location
[src/backend/utils/fmgr/funcapi.c:1379-1474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1379-L1474)

## Overview
Extracts comprehensive function argument information from a pg_proc catalog tuple, including argument types, names, and IN/OUT modes.

## Definition

```c
struct_array() since the array data is just going to look like
		 * a C array of values.
		 */
		arr = DatumGetArrayTypeP(proallargtypes);
```
## Detailed Description
This function retrieves complete argument metadata for a PostgreSQL function from its pg_proc system catalog entry. It handles both simple functions (with only IN parameters) and complex functions (with OUT, INOUT, and TABLE parameters) by examining the proallargtypes, proargnames, and proargmodes arrays.

The function prioritizes the proallargtypes array when available, falling back to proargtypes for simpler functions. It performs validation on array structure and dimensions to ensure data integrity. All returned data is palloc'd and becomes the caller's responsibility to free.

The function does not perform any interpretation of polymorphic types - it simply returns the raw type information as stored in the catalog.

## Parameters / Member Variables
- : HeapTuple pointing to the pg_proc catalog entry for the function
- : Output parameter receiving palloc'd array of argument type OIDs
- : Output parameter receiving palloc'd array of argument name strings (NULL if no names)
- : Output parameter receiving palloc'd array of argument modes (NULL if all IN)

## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - Form_pg_proc
  - [palloc](../p/palloc.md), memcpy
- Called from (representative examples):
  - [MatchNamedCall](../M/MatchNamedCall.md)
  - [print_function_arguments](../p/print_function_arguments.md)
  - [pg_get_function_arg_default](../p/pg_get_function_arg_default.md)
  - [print_function_sqlbody](../p/print_function_sqlbody.md)
  - [plperl_validator](../p/plperl_validator.md)
  - [PLy_procedure_create](../P/PLy_procedure_create.md)
  - [plsample_func_handler](../p/plsample_func_handler.md)

## Notes and Other Information
- Returns the total number of function arguments (including OUT parameters)
- Output arrays are set to NULL when corresponding catalog fields are not present
- Validates array structure and reports errors for malformed catalog data  
- The p_argtypes array is always populated, while p_argnames and p_argmodes may be NULL
- Essential for introspection of function signatures by various PostgreSQL subsystems
- Used extensively by procedural language handlers and system utilities for function analysis

## Simplified Source

```c
int get_func_arg_info(HeapTuple procTup,
                      Oid **p_argtypes, char ***p_argnames, char **p_argmodes) {
    Form_pg_proc procStruct = (Form_pg_proc) GETSTRUCT(procTup);
    int numargs;
    bool isNull;

    // Get argument types - check proallargtypes first, then proargtypes
    Datum proallargtypes = SysCacheGetAttr(PROCOID, procTup,
                                         Anum_pg_proc_proallargtypes, &isNull);
    if (!isNull) {
        // Use extended argument types (includes OUT parameters)
        ArrayType *arr = DatumGetArrayTypeP(proallargtypes);
        numargs = ARR_DIMS(arr)[0];

        // Validate array structure
        if (ARR_NDIM(arr) != 1 || numargs < 0 || ARR_HASNULL(arr) ||
            ARR_ELEMTYPE(arr) != OIDOID) {
            elog(ERROR, "proallargtypes is not a 1-D Oid array or it contains nulls");
        }

        *p_argtypes = (Oid *) palloc(numargs * sizeof(Oid));
        memcpy(*p_argtypes, ARR_DATA_PTR(arr), numargs * sizeof(Oid));
    } else {
        // Use basic argument types (IN parameters only)
        numargs = procStruct->proargtypes.dim1;
        *p_argtypes = (Oid *) palloc(numargs * sizeof(Oid));
        memcpy(*p_argtypes, procStruct->proargtypes.values, numargs * sizeof(Oid));
    }

    // Get argument names if available
    Datum proargnames = SysCacheGetAttr(PROCOID, procTup,
                                      Anum_pg_proc_proargnames, &isNull);
    if (isNull) {
        *p_argnames = NULL;
    } else {
        Datum *elems;
        int nelems;
        deconstruct_array_builtin(DatumGetArrayTypeP(proargnames), TEXTOID,
                                &elems, NULL, &nelems);

        *p_argnames = (char **) palloc(sizeof(char *) * numargs);
        for (int i = 0; i < numargs; i++) {
            (*p_argnames)[i] = TextDatumGetCString(elems[i]);
        }
    }

    // Get argument modes if available
    Datum proargmodes = SysCacheGetAttr(PROCOID, procTup,
                                      Anum_pg_proc_proargmodes, &isNull);
    if (isNull) {
        *p_argmodes = NULL;
    } else {
        ArrayType *arr = DatumGetArrayTypeP(proargmodes);

        // Validate modes array
        if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs ||
            ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID) {
            elog(ERROR, "proargmodes is not a 1-D char array of length %d", numargs);
        }

        *p_argmodes = (char *) palloc(numargs * sizeof(char));
        memcpy(*p_argmodes, ARR_DATA_PTR(arr), numargs * sizeof(char));
    }

    return numargs;
}
```