# array_to_json_internal

## Location
[src/backend/utils/adt/json.c:465-511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L465-L511)

## Overview
Converts a PostgreSQL array into JSON array format by extracting array metadata, deconstructing the array into individual elements, and recursively processing dimensions.

## Definition
```c
static void array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds)
```

## Detailed Description
array_to_json_internal serves as the main entry point for converting PostgreSQL arrays to JSON format. It handles the initial setup by extracting array metadata (dimensions, element type, size), determines the appropriate JSON conversion category for the element type, and deconstructcts the array into individual Datum values. For empty arrays, it returns "[]". For non-empty arrays, it calls array_dim_to_json to recursively process each dimension. The function manages memory by freeing the temporary arrays created during deconstruction.

## Parameters / Member Variables
- `array`: PostgreSQL Datum containing the array to convert
- `result`: StringInfo buffer where the JSON output is accumulated
- `use_line_feeds`: Boolean controlling whether to add line feeds for pretty formatting

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP (extract ArrayType from Datum)
  - ARR_ELEMTYPE, ARR_NDIM, ARR_DIMS (array metadata macros)
  - [ArrayGetNItems](../A/ArrayGetNItems.md) (calculate total number of elements)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md) (get type information for element type)
  - [json_categorize_type](../j/json_categorize_type.md) (determine JSON conversion approach)
  - [deconstruct_array](../d/deconstruct_array.md) (extract individual elements and null flags)
  - [array_dim_to_json](array_dim_to_json.md) (recursive dimension processing)
  - [pfree](../p/pfree.md) (memory cleanup)
- Called from (representative examples):
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [array_to_json](array_to_json.md)
  - [array_to_json_pretty](array_to_json_pretty.md)

## Notes and Other Information
The function optimizes for empty arrays by immediately returning "[]" without further processing. It properly handles PostgreSQL's array storage format by deconstructing the array using the element type's storage characteristics (length, by-value flag, alignment). Memory management is handled carefully with pfree calls to avoid leaks from the temporary element and nulls arrays created by deconstruct_array.

## Simplified Source

```c
static void array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds) {
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
        appendStringInfoString(result, "[]");
        return;
    }

    // Get element type information
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
    json_categorize_type(element_type, false, &tcategory, &outfuncoid);

    // Deconstruct array into individual elements
    deconstruct_array(v, element_type, typlen, typbyval, typalign,
                     &elements, &nulls, &nitems);

    // Recursively process array dimensions
    array_dim_to_json(result, 0, ndim, dim, elements, nulls, &count,
                     tcategory, outfuncoid, use_line_feeds);

    // Clean up temporary arrays
    pfree(elements);
    pfree(nulls);
}
```