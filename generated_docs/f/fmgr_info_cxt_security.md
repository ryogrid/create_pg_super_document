# fmgr_info_cxt_security

## Location
[src/backend/utils/fmgr/fmgr.c:147-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L147-L280)

## Overview
The core function that initializes FmgrInfo structs by looking up function metadata, handling both builtin and catalog-defined functions with optional security enforcement.

## Definition
```c
static void fmgr_info_cxt_security(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt, bool ignore_security)
```

## Detailed Description
fmgr_info_cxt_security is the workhorse function behind all FmgrInfo initialization in PostgreSQL. It performs a comprehensive lookup of function metadata and properly configures the FmgrInfo struct for efficient function calls. The function implements a two-tier lookup strategy: first checking if the function is a builtin (using fmgr_isbuiltin for O(1) lookup), then falling back to catalog lookup via pg_proc for user-defined and other functions.

The function handles multiple function languages (INTERNAL, C, SQL, and procedural languages) and implements security policies including SECURITY DEFINER functions and function hooks. When security features are needed, it delegates to fmgr_security_definer rather than setting up direct function calls. The function also manages function statistics tracking policies based on the function type and language.

## Parameters / Member Variables
- `functionId`: The Oid of the function to look up and initialize
- `finfo`: Pointer to the FmgrInfo struct to be initialized with function metadata
- `mcxt`: Memory context for allocating subsidiary data associated with this FmgrInfo
- `ignore_security`: Boolean flag to bypass security checks, used to prevent recursion in security-related functions

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_isbuiltin](fmgr_isbuiltin.md) (fast builtin function lookup)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (catalog access for pg_proc entries)
  - [heap_attisnull](../h/heap_attisnull.md) (checking for non-null proconfig values)
  - FmgrHookIsNeeded (checking if function hooks are required)
  - [fmgr_security_definer](fmgr_security_definer.md) (security definer call handler)
  - [fmgr_lookupByName](fmgr_lookupByName.md) (builtin function lookup by name for aliased internals)
  - [fmgr_info_C_lang](fmgr_info_C_lang.md) (C language function setup)
  - [fmgr_sql](fmgr_sql.md) (SQL language function handler)
  - [fmgr_info_other_lang](fmgr_info_other_lang.md) (procedural language function setup)
  - Various constants: TRACK_FUNC_ALL, TRACK_FUNC_PL, TRACK_FUNC_OFF
- Called from (representative examples):
  - [fmgr_info](fmgr_info.md) (convenience wrapper using current memory context)
  - [fmgr_info_cxt](fmgr_info_cxt.md) (wrapper with explicit memory context)
  - [fmgr_info_other_lang](fmgr_info_other_lang.md) (recursive call for procedural language setup)
  - [fmgr_security_definer](fmgr_security_definer.md) (recursive call with security bypass)

## Notes and Other Information
- This is a static function, only accessible within the fmgr.c file
- Implements the core logic for all function call setup in PostgreSQL
- Critical for security: properly handles SECURITY DEFINER functions and function hooks
- Optimized for builtin functions with fast-path lookup avoiding catalog access
- Handles function aliasing where user-created functions point to builtin internals
- Sets appropriate statistics tracking policies based on function language and type
- The fn_oid field is set last to ensure struct validity in case of errors
- Part of PostgreSQL's Function Manager (fmgr) subsystem responsible for function call dispatch
- Manages memory allocation through the specified memory context for long-term function info storage

## Simplified Source

```c
// Simplified version of fmgr_info_cxt_security
static void fmgr_info_cxt_security(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt, bool ignore_security) {
    const FmgrBuiltin *fbp;
    HeapTuple procedureTuple;
    Form_pg_proc procedureStruct;

    // Initialize FmgrInfo struct (fn_oid set last for validity)
    finfo->fn_oid = InvalidOid;
    finfo->fn_extra = NULL;
    finfo->fn_mcxt = mcxt;
    finfo->fn_expr = NULL;

    // Fast path: Check if this is a builtin function
    if ((fbp = fmgr_isbuiltin(functionId)) != NULL) {
        // Use builtin function metadata directly
        finfo->fn_nargs = fbp->nargs;
        finfo->fn_strict = fbp->strict;
        finfo->fn_retset = fbp->retset;
        finfo->fn_stats = TRACK_FUNC_ALL;  // Never track builtins
        finfo->fn_addr = fbp->func;
        finfo->fn_oid = functionId;
        return;
    }

    // Lookup function in pg_proc catalog
    procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
    if (!HeapTupleIsValid(procedureTuple))
        elog(ERROR, "cache lookup failed for function %u", functionId);
    procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

    // Copy basic function properties
    finfo->fn_nargs = procedureStruct->pronargs;
    finfo->fn_strict = procedureStruct->proisstrict;
    finfo->fn_retset = procedureStruct->proretset;

    // Check if security features are needed (SECURITY DEFINER, hooks, etc.)
    if (!ignore_security &&
        (procedureStruct->prosecdef ||
         !heap_attisnull(procedureTuple, Anum_pg_proc_proconfig, NULL) ||
         FmgrHookIsNeeded(functionId))) {
        // Use security definer wrapper
        finfo->fn_addr = fmgr_security_definer;
        finfo->fn_stats = TRACK_FUNC_ALL;
        finfo->fn_oid = functionId;
        ReleaseSysCache(procedureTuple);
        return;
    }

    // Set up function based on language
    switch (procedureStruct->prolang) {
        case INTERNALlanguageId:
            // Handle aliased builtin functions
            prosrc = get_function_source(procedureTuple);
            fbp = fmgr_lookupByName(prosrc);
            if (fbp == NULL)
                ereport(ERROR, "internal function not found in lookup table");
            finfo->fn_addr = fbp->func;
            finfo->fn_stats = TRACK_FUNC_ALL;
            break;

        case ClanguageId:
            // C language function
            fmgr_info_C_lang(functionId, finfo, procedureTuple);
            finfo->fn_stats = TRACK_FUNC_PL;
            break;

        case SQLlanguageId:
            // SQL language function
            finfo->fn_addr = fmgr_sql;
            finfo->fn_stats = TRACK_FUNC_PL;
            break;

        default:
            // Procedural language function
            fmgr_info_other_lang(functionId, finfo, procedureTuple);
            finfo->fn_stats = TRACK_FUNC_OFF;
            break;
    }

    // Finalize setup
    finfo->fn_oid = functionId;
    ReleaseSysCache(procedureTuple);
}
```

Key simplifications made:
- Removed detailed comments and consolidated initialization code
- Simplified error handling to focus on main logic flow
- Abstracted prosrc extraction into conceptual `get_function_source()` call
- Condensed security check conditions into single readable block
- Removed memory cleanup details (pfree calls) for clarity
- Focused on the core algorithm: builtin check → catalog lookup → security check → language-specific setup
- Preserved essential control flow and all major code paths