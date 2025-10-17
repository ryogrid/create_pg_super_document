# show_all_settings

## Location
[src/backend/utils/misc/guc_funcs.c:849-983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L849-L983)

## Overview
A PostgreSQL system function that returns a set of tuples containing all configuration parameters and their attributes, implementing the pg_settings system view.

## Definition

```c
struct config_generic **guc_vars;
```
## Detailed Description
This function implements the core functionality behind PostgreSQL's pg_settings system view, which provides comprehensive information about all configuration parameters. It's a Set Returning Function (SRF) that uses PostgreSQL's SRF framework to return multiple rows of data, each representing one configuration parameter with all its associated metadata.

The function operates in two phases: initialization (first call) where it sets up the tuple descriptor defining 17 columns matching the pg_settings view structure, and iteration (subsequent calls) where it processes each configuration parameter. It retrieves all GUC variables, filters out those marked as NO_SHOW_ALL or not visible to the current user, and uses GetConfigOptionValues() to extract formatted information for each visible parameter.

The function handles memory management carefully using PostgreSQL's multi-call memory context and implements proper SRF lifecycle management for efficient iteration over potentially hundreds of configuration parameters.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure (no arguments for this function)

Internal state variables:
- : Function call context for SRF state management
- : Array of configuration parameter structures
- : Total number of configuration parameters
- : Tuple descriptor defining the 17-column structure
- : Attribute metadata for tuple construction

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL, SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md), TupleDescInitEntry, TupleDescGetAttInMetadata
  - [get_guc_variables](../g/get_guc_variables.md)
  - [ConfigOptionIsVisible](../C/ConfigOptionIsVisible.md)
  - [GetConfigOptionValues](../G/GetConfigOptionValues.md)
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md), HeapTupleGetDatum
  - SRF_RETURN_NEXT, SRF_RETURN_DONE
- Called from (representative examples):
  - SQL queries using pg_settings view (no direct C references found)

## Notes and Other Information
- This function is the implementation behind the pg_settings system view that users query with SELECT * FROM pg_settings
- The function returns 17 columns: name, setting, unit, category, short_desc, extra_desc, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, sourcefile, sourceline, pending_restart
- Parameters marked with GUC_NO_SHOW_ALL flag are excluded from the results
- Visibility of parameters respects PostgreSQL's security model - some parameters may be hidden from non-privileged users
- The function uses PostgreSQL's SRF (Set Returning Function) framework for efficient streaming of large result sets
- Memory management is handled through PostgreSQL's multi-call memory context to prevent memory leaks across multiple function calls

## Simplified Source

```c
Datum
show_all_settings(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    struct config_generic **guc_vars;
    TupleDesc tupdesc;
    AttInMetadata *attinmeta;

    // First call: Initialize SRF context and setup
    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        // Switch to multi-call memory context
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Create tuple descriptor for 17 columns (pg_settings structure)
        tupdesc = CreateTemplateTupleDesc(NUM_PG_SETTINGS_ATTS);
        TupleDescInitEntry(tupdesc, 1, "name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "setting", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "unit", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "category", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 5, "short_desc", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 6, "extra_desc", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 7, "context", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 8, "vartype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 9, "source", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 10, "min_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 11, "max_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 12, "enumvals", TEXTARRAYOID, -1, 0);
        TupleDescInitEntry(tupdesc, 13, "boot_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 14, "reset_val", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 15, "sourcefile", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 16, "sourceline", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 17, "pending_restart", BOOLOID, -1, 0);

        // Setup attribute metadata for tuple construction
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        // Get all GUC variables and store in context
        int num_vars;
        guc_vars = get_guc_variables(&num_vars);
        funcctx->user_fctx = guc_vars;
        funcctx->max_calls = num_vars;

        MemoryContextSwitchTo(oldcontext);
    }

    // Per-call setup: Retrieve context and process next parameter
    funcctx = SRF_PERCALL_SETUP();
    guc_vars = (struct config_generic **) funcctx->user_fctx;
    attinmeta = funcctx->attinmeta;

    // Iterate through configuration parameters
    while (funcctx->call_cntr < funcctx->max_calls) {
        struct config_generic *conf = guc_vars[funcctx->call_cntr];

        // Skip hidden or non-visible parameters
        if ((conf->flags & GUC_NO_SHOW_ALL) || !ConfigOptionIsVisible(conf)) {
            funcctx->call_cntr++;
            continue;
        }

        // Extract parameter values and build tuple
        char *values[NUM_PG_SETTINGS_ATTS];
        GetConfigOptionValues(conf, (const char **) values);
        HeapTuple tuple = BuildTupleFromCStrings(attinmeta, values);
        Datum result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }

    // No more parameters to process
    SRF_RETURN_DONE(funcctx);
}
```