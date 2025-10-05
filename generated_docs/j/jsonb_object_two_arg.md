# jsonb_object_two_arg

## Location
[src/backend/utils/adt/jsonb.c:1379-1470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1379-L1470)

## Overview
SQL function that constructs a JSONB object from two separate text arrays - one containing keys and another containing values.

## Definition

```c
struct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);
```
## Detailed Description
The  function is a PostgreSQL SQL function that takes two separate one-dimensional text arrays as arguments: one for keys and one for values. It constructs a JSONB object by pairing elements from the two arrays positionally (first key with first value, second key with second value, etc.). The function validates that both arrays have the same dimensions and element count, ensuring proper key-value pairing while handling null values appropriately.

## Parameters / Member Variables
- Key array via : Text array containing object keys
- Value array via : Text array containing corresponding values

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract array arguments
  -  - Get array dimensions
  -  - Extract array elements
  -  - Build JSONB structure
  -  - Convert text datum to C string
  -  - Convert JsonbValue to final JSONB
  - Constants: , , , , , 
- Called from:
  - SQL queries using the jsonb_object(keys_array, values_array) function

## Notes and Other Information
- Requires both arrays to be one-dimensional with matching element counts
- Null keys are not permitted and will raise an error
- Null values are converted to JSON null values in the resulting object
- More intuitive than the single-array version when keys and values are naturally separate
- Uses JsonbInState for incremental JSONB construction
- Memory management includes freeing all temporary arrays after processing

## Simplified Source

```c
Datum
jsonb_object_two_arg(PG_FUNCTION_ARGS)
{
    ArrayType *key_array = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *val_array = PG_GETARG_ARRAYTYPE_P(1);
    int nkdims = ARR_NDIM(key_array);
    int nvdims = ARR_NDIM(val_array);
    Datum *key_datums, *val_datums;
    bool *key_nulls, *val_nulls;
    int key_count, val_count, i;
    JsonbInState result;

    // Initialize JSONB object construction
    memset(&result, 0, sizeof(JsonbInState));
    pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);

    // Validate array dimensions must match and be 1D
    if (nkdims > 1 || nkdims != nvdims)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("wrong number of array subscripts")));

    if (nkdims == 0)
        goto close_object;  // Empty arrays

    // Extract both arrays
    deconstruct_array_builtin(key_array, TEXTOID, &key_datums, &key_nulls, &key_count);
    deconstruct_array_builtin(val_array, TEXTOID, &val_datums, &val_nulls, &val_count);

    // Arrays must have same length
    if (key_count != val_count)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                       errmsg("mismatched array dimensions")));

    // Process key-value pairs
    for (i = 0; i < key_count; ++i) {
        JsonbValue v;
        char *str;
        int len;

        // Process key (null keys not allowed)
        if (key_nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("null value not allowed for object key")));

        str = TextDatumGetCString(key_datums[i]);
        len = strlen(str);
        v.type = jbvString;
        v.val.string.len = len;
        v.val.string.val = str;
        pushJsonbValue(&result.parseState, WJB_KEY, &v);

        // Process value (nulls become JSON null)
        if (val_nulls[i]) {
            v.type = jbvNull;
        } else {
            str = TextDatumGetCString(val_datums[i]);
            len = strlen(str);
            v.type = jbvString;
            v.val.string.len = len;
            v.val.string.val = str;
        }
        pushJsonbValue(&result.parseState, WJB_VALUE, &v);
    }

    // Cleanup arrays
    pfree(key_datums);
    pfree(key_nulls);
    pfree(val_datums);
    pfree(val_nulls);

close_object:
    result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);
    PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}
```