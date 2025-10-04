# compile_plperl_function

## Location
[src/pl/plperl/plperl.c:2718-2997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2718-L2997)

## Overview
Compiles or retrieves a cached PL/Perl function descriptor, handling the complete process from cache lookup to Perl code compilation and storage.

## Definition

```c
struct prodesc and subsidiary data must all live in proc_cxt.
		 ************************************************************/
		oldcontext = MemoryContextSwitchTo(proc_cxt);
```
## Detailed Description
This is a comprehensive function that manages the entire lifecycle of PL/Perl function compilation. It first attempts to find an existing cached function descriptor in the hash table, validating it against the current pg_proc entry. If no valid cached version exists, it creates a new function descriptor by analyzing the function's metadata, setting up memory contexts, processing argument and return types, extracting the source code, and compiling it in the appropriate Perl interpreter. The function handles both trusted (plperl) and untrusted (plperlu) variants, different function types (regular, trigger, event trigger), and includes comprehensive error handling with proper cleanup.

## Parameters / Member Variables
- : Object ID of the function to compile
- : Boolean indicating if this is a trigger function
- : Boolean indicating if this is an event trigger function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Retrieves pg_proc and related tuples
  - [hash_search](../h/hash_search.md): Searches and manages procedure hash table
  - [validate_plperl_function](../v/validate_plperl_function.md): Validates cached function descriptors
  - AllocSetContextCreate: Creates memory context for function data
  - [plperl_compile_callback](../p/plperl_compile_callback.md): Error callback for compilation errors
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)/SysCacheGetAttrNotNull: Extracts procedure attributes
  - [oid_array_to_list](../o/oid_array_to_list.md): Converts transform types array
  - [type_is_rowtype](../t/type_is_rowtype.md)/IsTrueArrayType: Type analysis functions
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md): Sets up function manager info
  - [getTypeIOParam](../g/getTypeIOParam.md): Gets type I/O parameters
  - TextDatumGetCString: Extracts function source code
  - [select_perl_context](../s/select_perl_context.md): Selects trusted/untrusted Perl context
  - [plperl_create_sub](../p/plperl_create_sub.md): Compiles Perl subroutine
  - [activate_interpreter](../a/activate_interpreter.md): Manages Perl interpreter state
  - increment_prodesc_refcount: Manages reference counting
  - [free_plperl_function](../f/free_plperl_function.md): Cleanup function for error cases
- Called from:
  - [plperl_validator](../p/plperl_validator.md): During function validation
  - [plperl_func_handler](../p/plperl_func_handler.md): For regular function execution
  - [plperl_trigger_handler](../p/plperl_trigger_handler.md): For trigger function execution
  - [plperl_event_trigger_handler](../p/plperl_event_trigger_handler.md): For event trigger execution

## Notes and Other Information
- Implements a two-tier caching strategy for plperl and plperlu functions
- Handles CREATE OR REPLACE FUNCTION by validating cached descriptors
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH/PG_END_TRY)
- Supports function polymorphism and transform types
- Validates argument and return types, rejecting inappropriate pseudotypes
- Manages memory contexts to prevent leaks during compilation errors
- Located at src/pl/plperl/plperl.c:2718-2997
- Critical function in PL/Perl's function management infrastructure

## Simplified Source

```c
static plperl_proc_desc *
compile_plperl_function(Oid fn_oid, bool is_trigger, bool is_event_trigger)
{
    HeapTuple procTup;
    Form_pg_proc procStruct;
    plperl_proc_key proc_key;
    plperl_proc_ptr *proc_ptr;
    plperl_proc_desc *prodesc = NULL;
    MemoryContext proc_cxt = NULL;
    plperl_interp_desc *oldinterp = plperl_active_interp;

    // Get function metadata from system catalog
    procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fn_oid));
    if (!HeapTupleIsValid(procTup))
        elog(ERROR, "cache lookup failed for function %u", fn_oid);
    procStruct = (Form_pg_proc) GETSTRUCT(procTup);

    // Try to find cached function (first plperl, then plperlu)
    proc_key.proc_id = fn_oid;
    proc_key.is_trigger = is_trigger;
    proc_key.user_id = GetUserId();

    proc_ptr = hash_search(plperl_proc_hash, &proc_key, HASH_FIND, NULL);
    if (validate_plperl_function(proc_ptr, procTup)) {
        ReleaseSysCache(procTup);
        return proc_ptr->proc_ptr;
    }

    // Try plperlu cache
    proc_key.user_id = InvalidOid;
    proc_ptr = hash_search(plperl_proc_hash, &proc_key, HASH_FIND, NULL);
    if (validate_plperl_function(proc_ptr, procTup)) {
        ReleaseSysCache(procTup);
        return proc_ptr->proc_ptr;
    }

    // Set up error handling for compilation
    ErrorContextCallback plperl_error_context;
    plperl_error_context.callback = plperl_compile_callback;
    plperl_error_context.previous = error_context_stack;
    plperl_error_context.arg = NameStr(procStruct->proname);
    error_context_stack = &plperl_error_context;

    PG_TRY();
    {
        // Create memory context for function data
        proc_cxt = AllocSetContextCreate(TopMemoryContext,
                                       "PL/Perl function",
                                       ALLOCSET_SMALL_SIZES);

        // Create and initialize function descriptor
        MemoryContext oldcontext = MemoryContextSwitchTo(proc_cxt);
        prodesc = (plperl_proc_desc *) palloc0(sizeof(plperl_proc_desc));
        prodesc->proname = pstrdup(NameStr(procStruct->proname));
        prodesc->fn_cxt = proc_cxt;
        prodesc->fn_refcount = 0;
        // ... set up basic metadata ...

        // Process return type (for non-triggers)
        if (!is_trigger && !is_event_trigger) {
            // Validate return type and set up conversion functions
            // ... return type processing ...
        }

        // Process argument types (for non-triggers)
        if (!is_trigger && !is_event_trigger) {
            // Validate argument types and set up conversion functions
            // ... argument type processing ...
        }

        // Get function source code
        Datum prosrcdatum = SysCacheGetAttrNotNull(PROCOID, procTup, Anum_pg_proc_prosrc);
        char *proc_source = TextDatumGetCString(prosrcdatum);

        // Compile in appropriate Perl interpreter
        select_perl_context(prodesc->lanpltrusted);
        prodesc->interp = plperl_active_interp;
        plperl_create_sub(prodesc, proc_source, fn_oid);
        activate_interpreter(oldinterp);

        pfree(proc_source);

        if (!prodesc->reference)
            elog(ERROR, "could not create PL/Perl internal procedure");

        // Add to hash table
        proc_key.user_id = prodesc->lanpltrusted ? GetUserId() : InvalidOid;
        proc_ptr = hash_search(plperl_proc_hash, &proc_key, HASH_ENTER, NULL);
        proc_ptr->proc_ptr = prodesc;
        increment_prodesc_refcount(prodesc);
    }
    PG_CATCH();
    {
        // Clean up on error
        if (prodesc && prodesc->reference)
            free_plperl_function(prodesc);
        else if (proc_cxt)
            MemoryContextDelete(proc_cxt);

        activate_interpreter(oldinterp);
        PG_RE_THROW();
    }
    PG_END_TRY();

    error_context_stack = plperl_error_context.previous;
    ReleaseSysCache(procTup);
    return prodesc;
}
```