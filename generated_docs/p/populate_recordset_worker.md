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