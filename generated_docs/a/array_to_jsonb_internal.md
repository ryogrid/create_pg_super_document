# array_to_jsonb_internal

## Location
[src/backend/utils/adt/jsonb.c:894-941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L894-L941)

## Overview
Converts a PostgreSQL ArrayType datum into JSONB format, handling multi-dimensional arrays by extracting array metadata and delegating to dimension processing.

## Definition
```c
static void array_to_jsonb_internal(Datum array, JsonbInState *result)
```

## Detailed Description
The `array_to_jsonb_internal` function serves as the entry point for converting PostgreSQL arrays to JSONB format. It takes an ArrayType datum and processes it through several stages:

1. **Array Inspection**: Extracts the ArrayType structure and determines basic properties like element type, dimensions, and total item count
2. **Empty Array Handling**: For arrays with zero elements, it creates an empty JSONB array and returns immediately
3. **Type Analysis**: Uses the element type to determine storage characteristics (typlen, typbyval, typalign) and JSON conversion category
4. **Array Deconstruction**: Flattens the multi-dimensional array into linear arrays of elements and null flags
5. **Dimension Processing**: Delegates to array_dim_to_jsonb to recursively process each dimension and build the nested JSONB structure
6. **Memory Cleanup**: Frees the temporary arrays allocated during deconstruction

The function handles arrays of any number of dimensions and any supported PostgreSQL data type, preserving the original structure in the resulting JSONB representation.

## Parameters / Member Variables
- `array`: Datum containing the PostgreSQL ArrayType to be converted
- `result`: JsonbInState structure to accumulate the conversion result

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ARR_ELEMTYPE
  - ARR_NDIM
  - ARR_DIMS
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [json_categorize_type](../j/json_categorize_type.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [array_dim_to_jsonb](array_dim_to_jsonb.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)

## Notes and Other Information
- This is a static function used internally within jsonb.c for array conversion
- Handles empty arrays as a special case by creating an empty JSONB array structure
- Uses PostgreSQL's array infrastructure to properly handle variable-length and fixed-length element types
- The function decomposes multi-dimensional arrays into a flat representation that array_dim_to_jsonb can process recursively
- Memory management includes freeing the temporary elements and nulls arrays created by deconstruct_array
- Supports arrays of any dimensionality supported by PostgreSQL (up to MAXDIM dimensions)
- The element type analysis determines how individual array elements will be converted to JSONB values

## Simplified Source

```c
static void array_to_jsonb_internal(Datum array, JsonbInState *result) {
    ArrayType *v = DatumGetArrayTypeP(array);
    Oid element_type = ARR_ELEMTYPE(v);
    int *dim;
    int ndim;
    int nitems;
    int count = 0;
    Datum *elements;
    bool *nulls;
    int16 typlen;
    bool typbyval;
    char typalign;
    JsonTypeCategory tcategory;
    Oid outfuncoid;

    // Extract array metadata
    ndim = ARR_NDIM(v);
    dim = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndim, dim);

    // Handle empty arrays
    if (nitems <= 0) {
        result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_ARRAY, NULL);
        result->res = pushJsonbValue(&result->parseState, WJB_END_ARRAY, NULL);
        return;
    }

    // Get element type information
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
    json_categorize_type(element_type, true, &tcategory, &outfuncoid);

    // Deconstruct array into individual elements
    deconstruct_array(v, element_type, typlen, typbyval, typalign,
                     &elements, &nulls, &nitems);

    // Recursively process array dimensions
    array_dim_to_jsonb(result, 0, ndim, dim, elements, nulls, &count,
                      tcategory, outfuncoid);

    // Clean up temporary arrays
    pfree(elements);
    pfree(nulls);
}
```