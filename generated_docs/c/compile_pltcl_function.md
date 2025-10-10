# compile_pltcl_function

## Location
[src/pl/tcl/pltcl.c:1400-1758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L1400-L1758)

## Overview
Compiles or retrieves a cached PL/Tcl function descriptor, handling function metadata extraction, Tcl procedure creation, and proper memory management.

## Definition
```c
static pltcl_proc_desc *
compile_pltcl_function(Oid fn_oid, Oid tgreloid, 
                      bool is_event_trigger, bool pltrusted)
```

## Detailed Description
This function serves as the central compilation and caching mechanism for PL/Tcl functions. It maintains a hash table of compiled function descriptors and handles the complete lifecycle of function preparation, from initial compilation through caching and cache validation.

The function performs several key operations: it first checks if a valid cached version exists by comparing transaction IDs and tuple IDs; if not found or outdated, it extracts function metadata from pg_proc, analyzes argument and return types, creates appropriate memory contexts, generates internal Tcl procedure names, builds the complete Tcl procedure definition with proper argument handling, and finally evaluates the procedure definition in the Tcl interpreter.

For different function types (regular functions, triggers, event triggers), it handles argument processing differently: regular functions get numbered parameters with type conversion setup, triggers receive predefined TG_* variables plus NEW/OLD tuple arrays, and event triggers get TG_event and TG_tag parameters. The function also performs comprehensive error handling with proper resource cleanup on failure.

## Parameters / Member Variables
- `fn_oid`: Object ID of the PostgreSQL function to compile
- `tgreloid`: Object ID of the relation for trigger functions, or InvalidOid for regular functions
- `is_event_trigger`: Boolean flag indicating if this is an event trigger function
- `pltrusted`: Boolean flag indicating if this is a trusted PL/Tcl function (affects interpreter selection)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [hash_search](../h/hash_search.md)
  - HeapTupleHeaderGetRawXmin
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - AllocSetContextCreate
  - [pltcl_fetch_interp](../p/pltcl_fetch_interp.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [getTypeIOParam](../g/getTypeIOParam.md)
  - [type_is_rowtype](../t/type_is_rowtype.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - Tcl_EvalEx
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [pltcl_func_handler](../p/pltcl_func_handler.md)
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md)
  - [pltcl_event_trigger_handler](../p/pltcl_event_trigger_handler.md)

## Notes and Other Information
- Uses hash table caching with procedure OID, trigger relation OID, and user ID as composite key
- Validates cached entries using transaction ID (xmin) and tuple ID comparison to detect CREATE OR REPLACE FUNCTION changes
- Creates separate memory context for each function descriptor to enable proper cleanup
- Generates unique internal procedure names using function OID and type suffixes (_trigger, _evttrigger)
- Handles reference counting for function descriptors to enable safe concurrent access and cleanup
- Disallows pseudotype arguments and returns except for specific allowed types (VOID, RECORD, etc.)
- Uses PG_TRY/PG_CATCH/PG_END_TRY for exception safety with proper resource cleanup
- Supports UTF-8 encoding conversion for function source code using UTF_E2U macro
- Creates comprehensive Tcl procedure definitions with upvar statements for global data access
- Implements different argument handling strategies for regular functions vs triggers vs event triggers

## Simplified Source

```c
static pltcl_proc_desc *compile_pltcl_function(Oid fn_oid, Oid tgreloid,
                                                bool is_event_trigger, bool pltrusted) {
    HeapTuple procTup;
    Form_pg_proc procStruct;
    pltcl_proc_key proc_key;
    pltcl_proc_ptr *proc_ptr;
    bool found;
    pltcl_proc_desc *prodesc;

    // Look up function in pg_proc catalog
    procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fn_oid));
    if (!HeapTupleIsValid(procTup))
        elog(ERROR, "cache lookup failed for function %u", fn_oid);
    procStruct = (Form_pg_proc) GETSTRUCT(procTup);

    // Check hash table cache for existing compiled function
    proc_key.proc_id = fn_oid;
    proc_key.is_trigger = OidIsValid(tgreloid);
    proc_key.user_id = pltrusted ? GetUserId() : InvalidOid;

    proc_ptr = hash_search(pltcl_proc_htab, &proc_key, HASH_ENTER, &found);
    if (!found)
        proc_ptr->proc_ptr = NULL;

    prodesc = proc_ptr->proc_ptr;

    // Check if cached version is still valid (handles CREATE OR REPLACE)
    if (prodesc != NULL &&
        prodesc->fn_xmin == HeapTupleHeaderGetRawXmin(procTup->t_data) &&
        ItemPointerEquals(&prodesc->fn_tid, &procTup->t_self)) {
        ReleaseSysCache(procTup);
        return prodesc;  // Use cached version
    }

    // Create new function descriptor
    MemoryContext proc_cxt = AllocSetContextCreate(TopMemoryContext,
                                                   "PL/Tcl function",
                                                   ALLOCSET_SMALL_SIZES);

    // Allocate and initialize procedure descriptor
    MemoryContext oldcontext = MemoryContextSwitchTo(proc_cxt);
    prodesc = (pltcl_proc_desc *) palloc0(sizeof(pltcl_proc_desc));

    // Set basic function metadata
    prodesc->user_proname = pstrdup(NameStr(procStruct->proname));
    prodesc->fn_cxt = proc_cxt;
    prodesc->fn_xmin = HeapTupleHeaderGetRawXmin(procTup->t_data);
    prodesc->fn_tid = procTup->t_self;
    prodesc->nargs = procStruct->pronargs;
    prodesc->fn_readonly = (procStruct->provolatile != PROVOLATILE_VOLATILE);
    prodesc->lanpltrusted = pltrusted;

    MemoryContextSwitchTo(oldcontext);

    // Get appropriate Tcl interpreter
    prodesc->interp_desc = pltcl_fetch_interp(procStruct->prolang, prodesc->lanpltrusted);
    Tcl_Interp *interp = prodesc->interp_desc->interp;

    // Setup return type information for regular functions
    if (!OidIsValid(tgreloid) && !is_event_trigger) {
        Oid rettype = procStruct->prorettype;
        // Validate return type and setup conversion functions
        prodesc->result_typid = rettype;
        prodesc->fn_retisset = procStruct->proretset;
        prodesc->fn_retistuple = type_is_rowtype(rettype);
    }

    // Setup argument type information for regular functions
    if (!OidIsValid(tgreloid) && !is_event_trigger) {
        prodesc->arg_out_func = (FmgrInfo *) palloc0(prodesc->nargs * sizeof(FmgrInfo));
        prodesc->arg_is_rowtype = (bool *) palloc0(prodesc->nargs * sizeof(bool));

        // Process each argument type
        for (int i = 0; i < prodesc->nargs; i++) {
            Oid argtype = procStruct->proargtypes.values[i];
            prodesc->arg_is_rowtype[i] = type_is_rowtype(argtype);
            if (!prodesc->arg_is_rowtype[i]) {
                // Setup output function for scalar types
                fmgr_info_cxt(get_type_output_func(argtype), &(prodesc->arg_out_func[i]), proc_cxt);
            }
        }
    }

    // Build Tcl procedure definition and arguments
    char internal_proname[128];
    if (is_event_trigger)
        snprintf(internal_proname, sizeof(internal_proname), "__PLTcl_proc_%u_evttrigger", fn_oid);
    else if (OidIsValid(tgreloid))
        snprintf(internal_proname, sizeof(internal_proname), "__PLTcl_proc_%u_trigger", fn_oid);
    else
        snprintf(internal_proname, sizeof(internal_proname), "__PLTcl_proc_%u", fn_oid);

    prodesc->internal_proname = pstrdup(internal_proname);

    // Get function source code and create Tcl procedure
    Datum prosrcdatum = SysCacheGetAttrNotNull(PROCOID, procTup, Anum_pg_proc_prosrc);
    char *proc_source = TextDatumGetCString(prosrcdatum);

    // Build complete Tcl procedure with proper argument setup
    Tcl_DString proc_def;
    Tcl_DStringInit(&proc_def);

    // Create the procedure in Tcl interpreter
    int tcl_rc = Tcl_EvalEx(interp, Tcl_DStringValue(&proc_def),
                           Tcl_DStringLength(&proc_def), TCL_EVAL_GLOBAL);

    if (tcl_rc != TCL_OK) {
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("could not create internal procedure \"%s\": %s",
                              internal_proname, utf_u2e(Tcl_GetStringResult(interp)))));
    }

    // Install in hash table and manage reference counting
    pltcl_proc_desc *old_prodesc = proc_ptr->proc_ptr;
    proc_ptr->proc_ptr = prodesc;
    prodesc->fn_refcount++;

    if (old_prodesc != NULL) {
        if (--old_prodesc->fn_refcount == 0)
            MemoryContextDelete(old_prodesc->fn_cxt);
    }

    Tcl_DStringFree(&proc_def);
    ReleaseSysCache(procTup);

    return prodesc;
}
```