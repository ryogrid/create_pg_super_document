# jsonb_object

## Location
[src/backend/utils/adt/jsonb.c:1279-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1279-L1378)

## Overview
SQL function that constructs a JSONB object from a one or two dimensional text array representing name-value pairs.

## Definition

```c
struct_array_builtin(in_array, TEXTOID, &in_datums, &in_nulls, &in_count);
```
## Detailed Description
The  function is a PostgreSQL SQL function that takes either a one-dimensional array with an even number of elements (representing alternating keys and values) or a two-dimensional array with exactly two columns (first column for keys, second for values) and constructs a JSONB object. The function validates array dimensions and ensures proper key-value pairing while handling null values appropriately - null keys are rejected with an error, while null values are converted to JSON null values.

## Parameters / Member Variables
- Input array via : Text array containing key-value pairs in one of two formats:
  - 1D array: [key1, value1, key2, value2, ...]  
  - 2D array: [[key1, value1], [key2, value2], ...]

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract array argument
  -  - Get array dimensions
  -  - Get array dimension sizes
  -  - Extract array elements
  -  - Build JSONB structure
  -  - Convert text datum to C string
  -  - Convert JsonbValue to final JSONB
  - Constants: , , , , , 
- Called from: 
  - SQL queries using the jsonb_object() function

## Notes and Other Information
- Validates array structure: 1D arrays must have even number of elements, 2D arrays must have exactly 2 columns
- Null keys are not permitted and will raise an error
- Null values are converted to JSON null values in the resulting object
- Uses JsonbInState for incremental JSONB construction
- Memory management includes freeing temporary arrays after processing

## Simplified Source

```c
Datum
jsonb_object(PG_FUNCTION_ARGS)
{
    ArrayType *in_array = PG_GETARG_ARRAYTYPE_P(0);
    int ndims = ARR_NDIM(in_array);
    Datum *in_datums;
    bool *in_nulls;
    int in_count, count, i;
    JsonbInState result;

    // Initialize JSONB object construction
    memset(&result, 0, sizeof(JsonbInState));
    pushJsonbValue(&result.parseState, WJB_BEGIN_OBJECT, NULL);

    // Validate array dimensions
    switch (ndims) {
        case 0:
            goto close_object;  // Empty array
        case 1:
            // 1D array must have even number of elements
            if ((ARR_DIMS(in_array)[0]) % 2)
                ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                               errmsg("array must have even number of elements")));
            break;
        case 2:
            // 2D array must have exactly 2 columns
            if ((ARR_DIMS(in_array)[1]) != 2)
                ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                               errmsg("array must have two columns")));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                           errmsg("wrong number of array subscripts")));
    }

    // Extract array elements
    deconstruct_array_builtin(in_array, TEXTOID, &in_datums, &in_nulls, &in_count);
    count = in_count / 2;

    // Process key-value pairs
    for (i = 0; i < count; ++i) {
        JsonbValue v;
        char *str;
        int len;

        // Process key (null keys not allowed)
        if (in_nulls[i * 2])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("null value not allowed for object key")));

        str = TextDatumGetCString(in_datums[i * 2]);
        len = strlen(str);
        v.type = jbvString;
        v.val.string.len = len;
        v.val.string.val = str;
        pushJsonbValue(&result.parseState, WJB_KEY, &v);

        // Process value (nulls become JSON null)
        if (in_nulls[i * 2 + 1]) {
            v.type = jbvNull;
        } else {
            str = TextDatumGetCString(in_datums[i * 2 + 1]);
            len = strlen(str);
            v.type = jbvString;
            v.val.string.len = len;
            v.val.string.val = str;
        }
        pushJsonbValue(&result.parseState, WJB_VALUE, &v);
    }

    // Cleanup and close object
    pfree(in_datums);
    pfree(in_nulls);

close_object:
    result.res = pushJsonbValue(&result.parseState, WJB_END_OBJECT, NULL);
    PG_RETURN_POINTER(JsonbValueToJsonb(result.res));
}
```