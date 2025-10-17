# fmgr_symbol

## Location
[src/backend/utils/fmgr/fmgr.c:281-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L281-L348)

## Overview
This function returns the module and C function name that provides the implementation of a given PostgreSQL function ID, supporting both core binary functions and extension shared objects.

## Definition

```c
void
fmgr_symbol(Oid functionId, char **mod, char **fn)
```
## Detailed Description
fmgr_symbol queries the PostgreSQL system catalog (pg_proc) to determine how a function with the given OID is implemented. It returns pointers to the module name and function name through output parameters. The function handles different language implementations:

- For security-defined functions or functions with configuration parameters, it returns "fmgr_security_definer" as a wrapper
- For INTERNAL language functions, it returns the function name from prosrc with mod=NULL (core binary)
- For C language functions, it returns both the shared library name (probin) and function name (prosrc)
- For SQL language functions, it returns "fmgr_sql" as the handler with mod=NULL
- For unknown languages, it returns NULL for both mod and fn

The returned strings are allocated in the current memory context using pstrdup.

## Parameters / Member Variables
- `functionId`: The OID of the function to look up in pg_proc catalog
- `**mod`: Output parameter for module/library name (NULL for core binary functions)
- `**fn`: Output parameter for C function name (NULL if no known implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [heap_attisnull](../h/heap_attisnull.md)
  - FmgrHookIsNeeded
  - [pstrdup](../p/pstrdup.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [llvm_function_reference](../l/llvm_function_reference.md) (in JIT compilation)
  - fmgr_info_set_expr (function info setup)

## Notes and Other Information
- This function is essential for PostgreSQL's function manager (fmgr) system
- It bridges the gap between function OIDs and their actual implementations
- The function handles security considerations by wrapping security-definer functions
- Memory management is handled through pstrdup allocation in the current context
- The function distinguishes between core PostgreSQL functions and extension functions

## Simplified Source

```c
void fmgr_symbol(Oid functionId, char **mod, char **fn)
{
    HeapTuple proc_tuple;
    Form_pg_proc proc_struct;
    Datum prosrc, probin;

    /* Look up function in pg_proc catalog */
    proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "cache lookup failed for function %u", functionId);

    proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);

    /* Handle security definer functions */
    if (proc_struct->prosecdef ||
        !heap_attisnull(proc_tuple, Anum_pg_proc_proconfig, NULL) ||
        FmgrHookIsNeeded(functionId)) {
        *mod = NULL;
        *fn = pstrdup("fmgr_security_definer");
        ReleaseSysCache(proc_tuple);
        return;
    }

    /* Determine function implementation based on language */
    switch (proc_struct->prolang) {
        case INTERNALlanguageId:
            prosrc = SysCacheGetAttrNotNull(PROCOID, proc_tuple, Anum_pg_proc_prosrc);
            *mod = NULL;  /* core binary */
            *fn = TextDatumGetCString(prosrc);
            break;

        case ClanguageId:
            prosrc = SysCacheGetAttrNotNull(PROCOID, proc_tuple, Anum_pg_proc_prosrc);
            probin = SysCacheGetAttrNotNull(PROCOID, proc_tuple, Anum_pg_proc_probin);
            *mod = TextDatumGetCString(probin);  /* shared library */
            *fn = TextDatumGetCString(prosrc);
            break;

        case SQLlanguageId:
            *mod = NULL;  /* core binary */
            *fn = pstrdup("fmgr_sql");
            break;

        default:
            *mod = NULL;
            *fn = NULL;  /* unknown language */
            break;
    }

    ReleaseSysCache(proc_tuple);
}
```