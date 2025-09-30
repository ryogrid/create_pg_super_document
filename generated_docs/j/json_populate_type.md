# json_populate_type

## Location
[src/backend/utils/adt/jsonfuncs.c:3343-3403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3343-L3403)

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
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [JsonbUnquote](../J/JsonbUnquote.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [populate_record_field](../p/populate_record_field.md)
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - VARSIZE
- Called from (representative examples):
  - [ExecEvalJsonCoercion](../E/ExecEvalJsonCoercion.md)
  - JsonTypeCategory

## Notes and Other Information
This function acts as a bridge between PostgreSQL's JSON processing and the general type conversion system. It efficiently handles both JSON text and binary JSONB formats by creating appropriate JsValue structures. The caching mechanism significantly improves performance for repeated conversions of the same type. The function supports PostgreSQL's soft error handling mechanism through the escontext parameter, allowing callers to handle conversion errors gracefully rather than throwing exceptions. The omit_quotes parameter is particularly useful for direct string conversions from JSON values.

## Simplified Source

```c
Datum
json_populate_type(Datum json_val, Oid json_type,
                   Oid typid, int32 typmod,
                   void **cache, MemoryContext mcxt,
                   bool *isnull, bool omit_quotes,
                   Node *escontext)
{
    JsValue jsv = {0};
    JsonbValue jbv;

    // Set up JsValue based on input type (JSON text vs JSONB binary)
    jsv.is_json = (json_type == JSONOID);

    if (*isnull) {
        // Handle NULL input
        jsv.val.json.str = NULL;
        jsv.val.jsonb = NULL;
    } else if (jsv.is_json) {
        // Handle JSON text input
        text *json = DatumGetTextPP(json_val);
        jsv.val.json.str = VARDATA_ANY(json);
        jsv.val.json.len = VARSIZE_ANY_EXHDR(json);
    } else {
        // Handle JSONB binary input
        Jsonb *jsonb = DatumGetJsonbP(json_val);
        jsv.val.jsonb = &jbv;

        if (omit_quotes) {
            // Strip quotes from string values
            char *str = JsonbUnquote(jsonb);
            jbv.type = jbvString;
            jbv.val.string.len = strlen(str);
            jbv.val.string.val = str;
        } else {
            // Use binary JSONB representation
            jbv.type = jbvBinary;
            jbv.val.binary.data = &jsonb->root;
            jbv.val.binary.len = VARSIZE(jsonb) - VARHDRSZ;
        }
    }

    // Initialize cache if needed
    if (*cache == NULL)
        *cache = MemoryContextAllocZero(mcxt, sizeof(ColumnIOData));

    // Delegate to record field population
    return populate_record_field(*cache, typid, typmod, NULL, mcxt,
                                 PointerGetDatum(NULL), &jsv, isnull,
                                 escontext, omit_quotes);
}
```