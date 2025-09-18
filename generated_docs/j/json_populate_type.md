# json_populate_type

## Location
src/backend/utils/adt/jsonfuncs.c: 3343 - 3403

## Overview
Populates and returns a PostgreSQL value of a specified type from a given JSON/JSONB value, handling both JSON text and binary JSONB formats with caching optimization.

## Definition
```c
Datum json_populate_type(Datum json_val, Oid json_type,
                        Oid typid, int32 typmod,
                        void **cache, MemoryContext mcxt,
                        bool *isnull, bool omit_quotes,
                        Node *escontext)
```

## Detailed Description
This function serves as a high-level interface for converting JSON/JSONB values to PostgreSQL data types. It handles both JSON text (JSONOID) and binary JSONB data, preparing appropriate JsValue structures for processing. The function maintains a cache of ColumnIOData for performance optimization across multiple calls with the same type. It supports error context for soft error handling and can optionally strip quotes from JSON strings. The actual type conversion is delegated to populate_record_field after setting up the appropriate data structures.

## Parameters / Member Variables
- `json_val`: The JSON/JSONB datum to be converted
- `json_type`: OID indicating whether input is JSON text (JSONOID) or JSONB binary
- `typid`: Target PostgreSQL type OID for conversion
- `typmod`: Type modifier for the target type
- `cache`: Pointer to cached ColumnIOData, allocated on first call and reused
- `mcxt`: Memory context for allocating cache and subsidiary memory
- `isnull`: Pointer to null flag, set if input is null
- `omit_quotes`: Boolean flag to strip quotes from JSON strings
- `escontext`: Error context for soft error handling, can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetTextPP
  - DatumGetJsonbP
  - JsonbUnquote
  - MemoryContextAllocZero
  - populate_record_field
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - VARSIZE
- Called from (representative examples):
  - ExecEvalJsonCoercion
  - JsonTypeCategory

## Notes and Other Information
This function acts as a bridge between PostgreSQL's JSON processing and the general type conversion system. It efficiently handles both JSON text and binary JSONB formats by creating appropriate JsValue structures. The caching mechanism significantly improves performance for repeated conversions of the same type. The function supports PostgreSQL's soft error handling mechanism through the escontext parameter, allowing callers to handle conversion errors gracefully rather than throwing exceptions. The omit_quotes parameter is particularly useful for direct string conversions from JSON values.