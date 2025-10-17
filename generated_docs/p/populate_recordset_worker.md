# populate_recordset_worker

## Location
[src/backend/utils/adt/jsonfuncs.c:4039-4212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4039-L4212)

## Overview
A static worker function that implements the core logic for converting JSON/JSONB arrays into PostgreSQL recordsets, serving as the common backend for multiple JSON recordset functions.

## Definition
```c
static Datum populate_recordset_worker(FunctionCallInfo fcinfo, const char *funcname,
                                      bool is_json, bool have_record_arg)
```

## Detailed Description
The `populate_recordset_worker` function is the central implementation for all JSON/JSONB to recordset conversion functions in PostgreSQL. It handles both JSON and JSONB inputs, manages memory contexts, validates input parameters, and coordinates the parsing and conversion process. The function sets up a tuple store for materialized results, processes the input JSON/JSONB array by either using JSON parsing callbacks or JSONB iteration, and returns the populated recordset. It includes comprehensive error handling and supports both explicit record type arguments and query-inferred record types.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing arguments and context
- `funcname`: String name of the calling function for error reporting
- `is_json`: Boolean indicating whether input is JSON (true) or JSONB (false)
- `have_record_arg`: Boolean indicating whether a record argument is provided for type information

## Dependencies
- Functions called/Symbols referenced:
  - [get_record_type_from_argument](../g/get_record_type_from_argument.md)
  - [get_record_type_from_query](../g/get_record_type_from_query.md)
  - [update_cached_tupdesc](../u/update_cached_tupdesc.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - [populate_recordset_array_start](populate_recordset_array_start.md) (and other JSON parsing callbacks)
  - pg_parse_json_or_ereport
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [populate_recordset_record](populate_recordset_record.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
- Called from (representative examples):
  - [jsonb_to_recordset](../j/jsonb_to_recordset.md)
  - [json_populate_recordset](../j/json_populate_recordset.md)
  - [json_to_recordset](../j/json_to_recordset.md)
  - [jsonb_populate_recordset](../j/jsonb_populate_recordset.md)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:4039-4212
- Static function, only accessible within the same compilation unit
- Implements the SFRM_Materialize return mode for set-returning functions
- Handles both RECORD and concrete types through caching mechanisms
- Validates that JSON/JSONB input is an array and contains objects
- Uses different parsing strategies for JSON (callback-based) vs JSONB (iterator-based)
- Memory management includes proper context switching for tuple store allocation
- Comprehensive error handling with descriptive error messages
- Critical component serving as the foundation for PostgreSQL`s JSON array to recordset functionality

## Simplified Source

```c
static Datum populate_recordset_worker(FunctionCallInfo fcinfo, const char *funcname,
                                      bool is_json, bool have_record_arg) {
    int json_arg_num = have_record_arg ? 1 : 0;
    ReturnSetInfo *rsi;
    HeapTupleHeader rec;
    PopulateRecordCache *cache = fcinfo->flinfo->fn_extra;
    PopulateRecordsetState *state;

    rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    // Validate function is called in set-returning context
    if (!rsi || !IsA(rsi, ReturnSetInfo) || !(rsi->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("materialize mode required")));
    }

    rsi->returnMode = SFRM_Materialize;

    // Initialize cache on first call
    if (!cache) {
        fcinfo->flinfo->fn_extra = cache =
            MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*cache));
        cache->fn_mcxt = fcinfo->flinfo->fn_mcxt;

        if (have_record_arg)
            get_record_type_from_argument(fcinfo, funcname, cache);
        else
            get_record_type_from_query(fcinfo, funcname, cache);
    }

    // Handle record argument for type information
    if (have_record_arg && !PG_ARGISNULL(0)) {
        rec = PG_GETARG_HEAPTUPLEHEADER(0);
        if (cache->argtype == RECORDOID) {
            cache->c.io.composite.base_typid = HeapTupleHeaderGetTypeId(rec);
            cache->c.io.composite.base_typmod = HeapTupleHeaderGetTypMod(rec);
        }
    } else {
        rec = NULL;
        if (cache->argtype == RECORDOID)
            get_record_type_from_query(fcinfo, funcname, cache);
    }

    // Return empty set for NULL JSON input
    if (PG_ARGISNULL(json_arg_num))
        PG_RETURN_NULL();

    // Update cached tuple descriptor
    update_cached_tupdesc(&cache->c.io.composite, cache->fn_mcxt);

    // Setup state and tuple store
    state = palloc0(sizeof(PopulateRecordsetState));
    state->tuple_store = tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
                                              false, work_mem);
    state->function_name = funcname;
    state->cache = cache;
    state->rec = rec;

    if (is_json) {
        // Parse JSON using callbacks
        text *json = PG_GETARG_TEXT_PP(json_arg_num);
        JsonLexContext lex;
        JsonSemAction *sem = palloc0(sizeof(JsonSemAction));

        makeJsonLexContext(&lex, json, true);

        // Setup JSON parsing callbacks
        sem->semstate = (void *) state;
        sem->array_start = populate_recordset_array_start;
        sem->array_element_start = populate_recordset_array_element_start;
        sem->scalar = populate_recordset_scalar;
        sem->object_field_start = populate_recordset_object_field_start;
        sem->object_field_end = populate_recordset_object_field_end;
        sem->object_start = populate_recordset_object_start;
        sem->object_end = populate_recordset_object_end;

        state->lex = &lex;
        pg_parse_json_or_ereport(&lex, sem);
        freeJsonLexContext(&lex);
    } else {
        // Parse JSONB using iterator
        Jsonb *jb = PG_GETARG_JSONB_P(json_arg_num);
        JsonbIterator *it;
        JsonbValue v;
        JsonbIteratorToken r;

        if (JB_ROOT_IS_SCALAR(jb) || !JB_ROOT_IS_ARRAY(jb)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("cannot call %s on a non-array", funcname)));
        }

        it = JsonbIteratorInit(&jb->root);

        // Process each array element as a record
        while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE) {
            if (r == WJB_ELEM) {
                JsObject obj;

                if (v.type != jbvBinary || !JsonContainerIsObject(v.val.binary.data)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                   errmsg("argument must be an array of objects")));
                }

                obj.is_json = false;
                obj.val.jsonb_cont = v.val.binary.data;
                populate_recordset_record(state, &obj);
            }
        }
    }

    // Return tuple store with results
    rsi->setResult = state->tuple_store;
    rsi->setDesc = CreateTupleDescCopy(cache->c.io.composite.tupdesc);

    PG_RETURN_NULL();
}
```