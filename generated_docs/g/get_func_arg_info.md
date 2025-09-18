# get_func_arg_info

## Location
src/backend/utils/fmgr/funcapi.c: 1379 - 1474

## Overview
Extracts comprehensive function argument information from a pg_proc catalog tuple, including argument types, names, and IN/OUT modes.

## Definition


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
  - SysCacheGetAttr
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - deconstruct_array_builtin
  - TextDatumGetCString
  - Form_pg_proc
  - palloc, memcpy
- Called from (representative examples):
  - MatchNamedCall
  - print_function_arguments
  - pg_get_function_arg_default
  - print_function_sqlbody
  - plperl_validator
  - PLy_procedure_create
  - plsample_func_handler

## Notes and Other Information
- Returns the total number of function arguments (including OUT parameters)
- Output arrays are set to NULL when corresponding catalog fields are not present
- Validates array structure and reports errors for malformed catalog data  
- The p_argtypes array is always populated, while p_argnames and p_argmodes may be NULL
- Essential for introspection of function signatures by various PostgreSQL subsystems
- Used extensively by procedural language handlers and system utilities for function analysis