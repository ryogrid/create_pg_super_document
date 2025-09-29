# build_function_result_tupdesc_t

## Location
[src/backend/utils/fmgr/funcapi.c:1705-1750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1705-L1750)

## Overview
Creates a tuple descriptor for a function's result rowtype from a pg_proc tuple, handling functions with OUT parameters that return RECORD.

## Definition
```c
TupleDesc build_function_result_tupdesc_t(HeapTuple procTuple)
```

## Detailed Description
This function takes a HeapTuple representing a row from the pg_proc system catalog and constructs a TupleDesc describing the result rowtype for functions that have OUT parameters. It serves as a wrapper around build_function_result_tupdesc_d, extracting the necessary argument information from the pg_proc tuple before delegating the actual tuple descriptor construction.

The function first validates that the function returns RECORD type and has the necessary argument metadata (proallargtypes and proargmodes). If these conditions are met, it extracts the function's argument types, modes, and names from the tuple and passes them to build_function_result_tupdesc_d for processing.

Note that this function deliberately does not handle resolution of polymorphic types, leaving that responsibility to the caller or higher-level functions.

## Parameters / Member Variables
- `procTuple`: A HeapTuple containing a row from the pg_proc system catalog representing the function whose result tuple descriptor is to be built

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (struct access via GETSTRUCT)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [build_function_result_tupdesc_d](build_function_result_tupdesc_d.md)
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [CallStmtResultDesc](../C/CallStmtResultDesc.md)
  - [internal_get_result_type](../i/internal_get_result_type.md)
  - TypeFuncClass

## Notes and Other Information
- Returns NULL if the function does not return RECORD type or lacks OUT parameters
- Extracts proallargtypes, proargmodes, and proargnames from the pg_proc tuple
- Handles the case where proargnames might be null by explicitly setting it to NULL
- Acts as a tuple-based interface to the more general build_function_result_tupdesc_d function
- Used during function creation and result type analysis
- Does not resolve polymorphic types - this must be handled separately if needed

## Simplified Source

```c
TupleDesc build_function_result_tupdesc_t(HeapTuple procTuple)
{
    Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(procTuple);
    Datum proallargtypes;
    Datum proargmodes;
    Datum proargnames;
    bool isnull;

    // Only handle functions that return RECORD
    if (procform->prorettype != RECORDOID)
        return NULL;

    // Check if function has OUT parameters (requires proallargtypes and proargmodes)
    if (heap_attisnull(procTuple, Anum_pg_proc_proallargtypes, NULL) ||
        heap_attisnull(procTuple, Anum_pg_proc_proargmodes, NULL))
        return NULL;

    // Extract argument metadata from pg_proc tuple
    proallargtypes = SysCacheGetAttrNotNull(PROCOID, procTuple,
                                            Anum_pg_proc_proallargtypes);
    proargmodes = SysCacheGetAttrNotNull(PROCOID, procTuple,
                                         Anum_pg_proc_proargmodes);
    proargnames = SysCacheGetAttr(PROCOID, procTuple,
                                  Anum_pg_proc_proargnames,
                                  &isnull);
    if (isnull)
        proargnames = PointerGetDatum(NULL);

    // Delegate actual tuple descriptor construction
    return build_function_result_tupdesc_d(procform->prokind,
                                           proallargtypes,
                                           proargmodes,
                                           proargnames);
}
```