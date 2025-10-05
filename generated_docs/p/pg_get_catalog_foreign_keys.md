# pg_get_catalog_foreign_keys

## Location
[src/backend/utils/adt/misc.c:496-563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L496-L563)

## Overview
Returns information about foreign key relationships between PostgreSQL system catalog tables that are not explicitly defined as formal foreign key constraints.

## Definition
```c
Datum pg_get_catalog_foreign_keys(PG_FUNCTION_ARGS)
```

## Detailed Description
This set-returning function exposes the implicit foreign key relationships that exist between PostgreSQL's system catalog tables. While these relationships are not enforced by actual foreign key constraints (for performance and bootstrap reasons), they represent logical dependencies between catalog tables.

The function iterates through the sys_fk_relationships array and returns detailed information about each relationship:

1. **fk_table**: OID of the table containing the foreign key columns
2. **fk_columns**: Text array of column names that reference the primary key
3. **pk_table**: OID of the table being referenced (contains the primary key)
4. **pk_columns**: Text array of primary key column names being referenced  
5. **is_array**: Boolean indicating if the foreign key reference involves array matching
6. **is_opt**: Boolean indicating if the foreign key reference is optional (nullable)

This information is crucial for understanding the logical structure of the system catalogs and is used by tools that need to navigate catalog relationships, such as pg_dump, system administration utilities, and query planners.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
  - [get_call_result_type](../g/get_call_result_type.md)
  - [BlessTupleDesc](../B/BlessTupleDesc.md)
  - [fmgr_info](../f/fmgr_info.md)
  - FunctionCall3
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - lengthof
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
- Global data structures referenced:
  - sys_fk_relationships (array of SysFKRelationship structs)
- Constants referenced:
  - F_ARRAY_IN
  - TEXTOID
  - TYPEFUNC_COMPOSITE
- Called from:
  - SQL function calls (no direct C references found)

## Notes and Other Information
- This function exposes metadata about PostgreSQL's internal catalog structure that isn't available through standard information_schema views
- The relationships returned are logical/semantic rather than enforced constraints - actual FK constraints would impact system catalog performance
- Used by system administration tools, database introspection utilities, and dependency analysis tools
- The sys_fk_relationships array is statically defined and contains hardcoded knowledge about catalog relationships
- Array conversion is handled through the array_in function to properly format column lists as PostgreSQL text arrays
- The is_array and is_opt flags provide additional semantic information about the nature of each foreign key relationship
- Memory management uses the SRF multi-call context to maintain state across multiple function calls

## Simplified Source

```c
Datum pg_get_catalog_foreign_keys(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    FmgrInfo *arrayinp;

    if (SRF_IS_FIRSTCALL()) {
        // Initialize set-returning function context
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Validate return type and setup tuple descriptor
        TupleDesc tupdesc;
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        // Setup array_in function for converting column lists to text arrays
        arrayinp = (FmgrInfo *) palloc(sizeof(FmgrInfo));
        fmgr_info(F_ARRAY_IN, arrayinp);
        funcctx->user_fctx = arrayinp;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    arrayinp = (FmgrInfo *) funcctx->user_fctx;

    // Return next foreign key relationship if available
    if (funcctx->call_cntr < lengthof(sys_fk_relationships)) {
        const SysFKRelationship *fkrel = &sys_fk_relationships[funcctx->call_cntr];
        Datum values[6];
        bool nulls[6];

        memset(nulls, false, sizeof(nulls));

        // Fill in the relationship data
        values[0] = ObjectIdGetDatum(fkrel->fk_table);
        values[1] = FunctionCall3(arrayinp,
                CStringGetDatum(fkrel->fk_columns),
                ObjectIdGetDatum(TEXTOID),
                Int32GetDatum(-1));
        values[2] = ObjectIdGetDatum(fkrel->pk_table);
        values[3] = FunctionCall3(arrayinp,
                CStringGetDatum(fkrel->pk_columns),
                ObjectIdGetDatum(TEXTOID),
                Int32GetDatum(-1));
        values[4] = BoolGetDatum(fkrel->is_array);
        values[5] = BoolGetDatum(fkrel->is_opt);

        HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
```