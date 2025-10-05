# json_object_two_arg

## Location
[src/backend/utils/adt/json.c:1485-1562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1485-L1562)

## Overview
SQL function that constructs JSON objects from two separate PostgreSQL text arrays, one for keys and one for values.

## Definition

```c
struct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);
```
## Detailed Description
The `json_object_two_arg` function is a PostgreSQL SQL function that creates JSON objects from two separate one-dimensional text arrays - one containing keys and another containing corresponding values. It validates that both arrays have the same dimensions and element counts, then constructs a JSON object by pairing elements at matching indices. The function ensures proper JSON formatting by escaping keys and values and handles null values appropriately.

## Parameters / Member Variables
- Key array accessed via `PG_GETARG_ARRAYTYPE_P(0)`: Text array containing JSON object keys
- Value array accessed via `PG_GETARG_ARRAYTYPE_P(1)`: Text array containing JSON object values

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM
  - PG_RETURN_DATUM
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - [escape_json](../e/escape_json.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- Returns empty JSON object "{}" for zero-dimensional arrays
- Requires both arrays to be one-dimensional and have matching element counts
- Enforces strict validation: mismatched array dimensions trigger ERRCODE_ARRAY_SUBSCRIPT_ERROR
- Null keys are forbidden and trigger ERRCODE_NULL_VALUE_NOT_ALLOWED error
- Null values in the value array are converted to JSON null literals
- Uses escape_json function to properly escape both keys and values according to JSON standards
- Provides cleaner interface compared to json_object for cases where keys and values are naturally separate
- Includes comprehensive memory management with proper cleanup of all allocated arrays and buffers

## Simplified Source

```c
Datum json_object_two_arg(PG_FUNCTION_ARGS) {
    ArrayType *key_array = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *val_array = PG_GETARG_ARRAYTYPE_P(1);
    int nkdims = ARR_NDIM(key_array);
    int nvdims = ARR_NDIM(val_array);
    StringInfoData result;
    Datum *key_datums, *val_datums;
    bool *key_nulls, *val_nulls;
    int key_count, val_count, i;
    text *rval;
    char *v;

    // Validate array dimensions
    if (nkdims > 1 || nkdims != nvdims)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("wrong number of array subscripts")));

    if (nkdims == 0)
        PG_RETURN_DATUM(CStringGetTextDatum("{}"));

    // Extract arrays
    deconstruct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);
    deconstruct_array_builtin(val_array, TEXTOID, &val_datums, &val_nulls, &val_count);

    // Validate matching counts
    if (key_count != val_count)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("mismatched array dimensions")));

    // Build JSON object string
    initStringInfo(&result);
    appendStringInfoChar(&result, '{');

    for (i = 0; i < key_count; ++i) {
        // Reject null keys
        if (key_nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("null value not allowed for object key")));

        // Add key
        v = TextDatumGetCString(key_datums[i]);
        if (i > 0)
            appendStringInfoString(&result, ", ");
        escape_json(&result, v);
        appendStringInfoString(&result, " : ");
        pfree(v);

        // Add value (or null)
        if (val_nulls[i])
            appendStringInfoString(&result, "null");
        else {
            v = TextDatumGetCString(val_datums[i]);
            escape_json(&result, v);
            pfree(v);
        }
    }

    appendStringInfoChar(&result, '}');

    // Cleanup and return
    pfree(key_datums);
    pfree(key_nulls);
    pfree(val_datums);
    pfree(val_nulls);
    rval = cstring_to_text_with_len(result.data, result.len);
    pfree(result.data);
    PG_RETURN_TEXT_P(rval);
}
```