# populate_composite

## Location
[src/backend/utils/adt/jsonfuncs.c:3056-3122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3056-L3122)

## Overview
Recursively populates a composite (row type) value from JSON/JsonB data, handling both regular composite types and domain types over composites.

## Definition
```c
static Datum populate_composite(CompositeIOData *io,
                               Oid typid,
                               const char *colname,
                               MemoryContext mcxt,
                               HeapTupleHeader defaultval,
                               JsValue *jsv,
                               bool *isnull,
                               Node *escontext)
```

## Detailed Description
populate_composite orchestrates the conversion of JSON/JsonB data into PostgreSQL composite type values. The function operates in several key phases:

1. **Cache Management**: Updates the tuple descriptor cache to ensure current type information
2. **Input Validation**: Converts the JsValue to a JsObject structure via JsValueToJsObject()
3. **Record Population**: Uses populate_record() to create the actual tuple from the object data
4. **Domain Validation**: For domain types over composites, applies domain constraints using domain_check_safe()

The function supports both NULL handling and soft error reporting through the ErrorSaveContext mechanism. It properly manages memory contexts and ensures cleanup of temporary objects. The recursive nature allows for nested composite types to be handled correctly.

## Parameters / Member Variables
- `io`: CompositeIOData structure containing cached type information and record I/O data
- `typid`: Target type OID for the result (may differ from base type for domain types)
- `colname`: Column name for error reporting (currently unused in implementation)
- `mcxt`: Memory context for allocating the result data
- `defaultval`: Default tuple header to use for missing values
- `jsv`: Input JSON/JsonB value to be converted
- `isnull`: Pointer to NULL indicator flag (input/output)
- `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [update_cached_tupdesc](../u/update_cached_tupdesc.md)
  - [JsValueToJsObject](../J/JsValueToJsObject.md)
  - [populate_record](populate_record.md)
  - [HeapTupleHeaderGetDatum](../H/HeapTupleHeaderGetDatum.md)
  - JsObjectFree
  - [domain_check_safe](../d/domain_check_safe.md)
  - SOFT_ERROR_OCCURRED
- Called from (representative examples):
  - [populate_record_field](populate_record_field.md)
  - [populate_record_worker](populate_record_worker.md)
  - JsObjectFree

## Notes and Other Information
- Returns NULL datum and sets *isnull = true on any error when using soft error handling
- Automatically detects domain types by comparing typid to base_typid and applies appropriate constraint checking
- Uses HeapTupleHeaderGetDatum() to flatten any TOAST references in the result
- Ensures proper cleanup by calling JsObjectFree() after record population
- The colname parameter exists for consistency with other populate functions but is not currently used in error reporting
- Domain constraint checking is skipped for RECORDOID to avoid unnecessary overhead for anonymous records
- Supports recursive population for nested composite structures

## Simplified Source

```c
static Datum populate_composite(CompositeIOData *io, Oid typid,
                               const char *colname, MemoryContext mcxt,
                               HeapTupleHeader defaultval, JsValue *jsv,
                               bool *isnull, Node *escontext) {
    Datum result;

    // Update cached tuple descriptor
    update_cached_tupdesc(io, mcxt);

    if (*isnull) {
        result = (Datum) 0;
    } else {
        HeapTupleHeader tuple;
        JsObject jso;

        // Convert JSON/JsonB value to object
        if (!JsValueToJsObject(jsv, &jso, escontext)) {
            *isnull = true;
            return (Datum) 0;
        }

        // Populate record tuple from JSON object
        tuple = populate_record(io->tupdesc, &io->record_io,
                               defaultval, mcxt, &jso, escontext);

        if (SOFT_ERROR_OCCURRED(escontext)) {
            *isnull = true;
            return (Datum) 0;
        }

        result = HeapTupleHeaderGetDatum(tuple);
        JsObjectFree(&jso);
    }

    // Apply domain constraints if this is a domain over composite
    if (typid != io->base_typid && typid != RECORDOID) {
        if (!domain_check_safe(result, *isnull, typid, &io->domain_info,
                              mcxt, escontext)) {
            *isnull = true;
            return (Datum) 0;
        }
    }

    return result;
}
```