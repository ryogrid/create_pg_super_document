# populate_record_worker

## Location
[src/backend/utils/adt/jsonfuncs.c:3697-3808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3697-L3808)

## Overview
A common worker function that implements the core logic for JSON/JSONB to record conversion functions, handling both populate and to_record variants with comprehensive type resolution and caching.

## Definition

```c
static Datum
populate_record_worker(FunctionCallInfo fcinfo, const char *funcname,
					   bool is_json, bool have_record_arg,
					   Node *escontext)
```
## Detailed Description
This function serves as the unified implementation for multiple JSON record conversion functions including , , , and . It handles type resolution, caching, input validation, and delegates the actual conversion work to . The function manages different input scenarios, including cases where record types must be inferred from query context.

Key behaviors:
- Initializes and manages per-function-call caching for performance
- Handles type resolution from arguments or query context
- Supports both JSON text and JSONB binary formats
- Manages null inputs and returns appropriate results
- Delegates actual conversion to populate_composite
- Handles RECORD type resolution at runtime

## Parameters / Member Variables
- `fcinfo`: Function call information containing arguments and execution context
- `*funcname`: Name of the calling function (for error reporting)
- `is_json`: Boolean indicating whether input is JSON text (true) or JSONB (false)
- `have_record_arg`: Boolean indicating whether function has a record argument (populate functions vs to_record functions)
- `*escontext`: Error context for soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [get_record_type_from_argument](../g/get_record_type_from_argument.md)
  - [get_record_type_from_query](../g/get_record_type_from_query.md)
  - PG_GETARG_HEAPTUPLEHEADER
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - PG_GETARG_TEXT_PP
  - PG_GETARG_JSONB_P
  - [populate_composite](populate_composite.md)
  - SOFT_ERROR_OCCURRED
  - PG_RETURN_DATUM
- Called from (representative examples):
  - [jsonb_populate_record](../j/jsonb_populate_record.md)
  - [jsonb_populate_record_valid](../j/jsonb_populate_record_valid.md)
  - [jsonb_to_record](../j/jsonb_to_record.md)
  - [json_populate_record](../j/json_populate_record.md)
  - [json_to_record](../j/json_to_record.md)

## Notes and Other Information
- This is a static function serving as the common implementation for multiple public functions
- Implements sophisticated caching to avoid repeated type lookups within a query
- Handles the distinction between JSON text and JSONB binary formats
- Supports soft error handling through the escontext parameter
- Manages runtime type resolution for RECORD types
- Returns unchanged record for null JSON inputs
- Part of PostgreSQL's comprehensive JSON/JSONB to record conversion infrastructure
- The function signature uses boolean flags to distinguish between different calling patterns

## Simplified Source

```c
static Datum
populate_record_worker(FunctionCallInfo fcinfo, const char *funcname,
                       bool is_json, bool have_record_arg,
                       Node *escontext)
{
    int json_arg_num = have_record_arg ? 1 : 0;
    JsValue jsv = {0};
    HeapTupleHeader rec;
    Datum rettuple;
    bool isnull;
    JsonbValue jbv;
    MemoryContext fnmcxt = fcinfo->flinfo->fn_mcxt;
    PopulateRecordCache *cache = fcinfo->flinfo->fn_extra;

    // Initialize cache on first call
    if (!cache) {
        fcinfo->flinfo->fn_extra = cache =
            MemoryContextAllocZero(fnmcxt, sizeof(*cache));
        cache->fn_mcxt = fnmcxt;

        // Determine record type from argument or query context
        if (have_record_arg)
            get_record_type_from_argument(fcinfo, funcname, cache);
        else
            get_record_type_from_query(fcinfo, funcname, cache);
    }

    // Handle record argument
    if (!have_record_arg)
        rec = NULL; // json{b}_to_record() case
    else if (!PG_ARGISNULL(0)) {
        rec = PG_GETARG_HEAPTUPLEHEADER(0);

        // For RECORD type, get actual type from tuple header
        if (cache->argtype == RECORDOID) {
            cache->c.io.composite.base_typid = HeapTupleHeaderGetTypeId(rec);
            cache->c.io.composite.base_typmod = HeapTupleHeaderGetTypMod(rec);
        }
    } else {
        rec = NULL;

        // For null RECORD argument, get type from query context
        if (cache->argtype == RECORDOID) {
            get_record_type_from_query(fcinfo, funcname, cache);
            Assert(cache->argtype == RECORDOID);
        }
    }

    // Handle null JSON input - return record unchanged
    if (PG_ARGISNULL(json_arg_num)) {
        if (rec)
            PG_RETURN_POINTER(rec);
        else
            PG_RETURN_NULL();
    }

    // Setup JSON/JSONB value for processing
    jsv.is_json = is_json;

    if (is_json) {
        // JSON text format
        text *json = PG_GETARG_TEXT_PP(json_arg_num);
        jsv.val.json.str = VARDATA_ANY(json);
        jsv.val.json.len = VARSIZE_ANY_EXHDR(json);
        jsv.val.json.type = JSON_TOKEN_INVALID;
    } else {
        // JSONB binary format
        Jsonb *jb = PG_GETARG_JSONB_P(json_arg_num);
        jsv.val.jsonb = &jbv;
        jbv.type = jbvBinary;
        jbv.val.binary.data = &jb->root;
        jbv.val.binary.len = VARSIZE(jb) - VARHDRSZ;
    }

    // Perform the actual conversion
    isnull = false;
    rettuple = populate_composite(&cache->c.io.composite, cache->argtype,
                                  NULL, fnmcxt, rec, &jsv, &isnull,
                                  escontext);
    Assert(!isnull || SOFT_ERROR_OCCURRED(escontext));

    PG_RETURN_DATUM(rettuple);
}
```