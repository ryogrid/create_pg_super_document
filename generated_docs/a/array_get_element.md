# array_get_element

## Location
[src/backend/utils/adt/arrayfuncs.c:1820-1920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1820-L1920)

## Overview
Extracts a single element from an array datum using provided subscripts, handling both ordinary varlena arrays and fixed-length arrays.

## Definition
```c
Datum array_get_element(Datum arraydatum,
                       int nSubscripts,
                       int *indx,
                       int arraytyplen,
                       int elmlen,
                       bool elmbyval,
                       char elmalign,
                       bool *isNull)
```

## Detailed Description
The `array_get_element` function is a core array access routine that retrieves a specific element from a PostgreSQL array using subscript indices. It handles three distinct array types: fixed-length arrays (assumed to be 1-dimensional and 0-based), expanded arrays (delegated to `array_get_element_expanded`), and normal varlena arrays. The function performs bounds checking, calculates the linear offset from multi-dimensional indices, and handles NULL elements appropriately. For pass-by-reference datatypes, it returns a pointer into the array object rather than copying the data.

## Parameters / Member Variables
- `arraydatum`: The array object datum (must not be NULL)
- `nSubscripts`: Number of subscripts supplied in the indx array
- `indx[]`: Array of subscript values for each dimension
- `arraytyplen`: pg_type.typlen for the array type (>0 for fixed-length arrays)
- `elmlen`: pg_type.typlen for the array's element type
- `elmbyval`: pg_type.typbyval for the array's element type (pass-by-value flag)
- `elmalign`: pg_type.typalign for the array's element type (alignment requirement)
- `*isNull`: Output parameter set to indicate whether the element is NULL

## Dependencies
- Functions called/Symbols referenced:
  - `VARATT_IS_EXTERNAL_EXPANDED`: Checks if array is in expanded format
  - [array_get_element_expanded](array_get_element_expanded.md): Handles expanded arrays
  - `DatumGetArrayTypeP`: Converts datum to ArrayType pointer
  - `ARR_NDIM`, `ARR_DIMS`, `ARR_LBOUND`: Array metadata accessors
  - `ARR_DATA_PTR`, `ARR_NULLBITMAP`: Array data accessors
  - `[ArrayGetOffset](../A/ArrayGetOffset.md)`: Calculates linear offset from subscripts
  - `[array_get_isnull](array_get_isnull.md)`: Checks if specific element is NULL
  - `[array_seek](array_seek.md)`: Seeks to element position in array data
  - `[ArrayCast](../A/ArrayCast.md)`: Casts array element data to Datum
- Called from (representative examples):
  - [array_ref](array_ref.md): Array reference operations
  - [array_subscript_fetch](array_subscript_fetch.md): Array subscripting operations
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md): Column type alteration
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md): Tuple descriptor building

## Notes and Other Information
- Returns (Datum) 0 with *isNull=true for invalid subscripts or NULL elements
- Supports multi-dimensional arrays up to MAXDIM dimensions
- Performs comprehensive bounds checking against array dimensions and lower bounds
- Fixed-length arrays are treated as 1-dimensional with 0-based indexing
- For expanded arrays, delegates to specialized handling function
- Critical function for PostgreSQL's array element access infrastructure
- Located in `src/backend/utils/adt/arrayfuncs.c` at lines 1820-1920

## Simplified Source

```c
Datum array_get_element(Datum arraydatum, int nSubscripts, int *indx,
                       int arraytyplen, int elmlen, bool elmbyval,
                       char elmalign, bool *isNull) {
    int ndim, *dim, *lb, offset;
    int fixedDim[1], fixedLb[1];
    char *arraydataptr, *retptr;
    bits8 *arraynullsptr;

    // Handle fixed-length arrays (1-d, 0-based)
    if (arraytyplen > 0) {
        ndim = 1;
        fixedDim[0] = arraytyplen / elmlen;
        fixedLb[0] = 0;
        dim = fixedDim;
        lb = fixedLb;
        arraydataptr = (char *) DatumGetPointer(arraydatum);
        arraynullsptr = NULL;
    }
    // Handle expanded arrays
    else if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(arraydatum))) {
        return array_get_element_expanded(arraydatum, nSubscripts, indx,
                                        arraytyplen, elmlen, elmbyval, elmalign, isNull);
    }
    // Handle normal varlena arrays
    else {
        ArrayType *array = DatumGetArrayTypeP(arraydatum);
        ndim = ARR_NDIM(array);
        dim = ARR_DIMS(array);
        lb = ARR_LBOUND(array);
        arraydataptr = ARR_DATA_PTR(array);
        arraynullsptr = ARR_NULLBITMAP(array);
    }

    // Validate subscripts
    if (ndim != nSubscripts || ndim <= 0 || ndim > MAXDIM) {
        *isNull = true;
        return (Datum) 0;
    }

    // Check bounds for each dimension
    for (int i = 0; i < ndim; i++) {
        if (indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i])) {
            *isNull = true;
            return (Datum) 0;
        }
    }

    // Calculate element offset and check for NULL
    offset = ArrayGetOffset(nSubscripts, dim, lb, indx);

    if (array_get_isnull(arraynullsptr, offset)) {
        *isNull = true;
        return (Datum) 0;
    }

    // Get the element data
    *isNull = false;
    retptr = array_seek(arraydataptr, 0, arraynullsptr, offset,
                        elmlen, elmbyval, elmalign);
    return ArrayCast(retptr, elmbyval, elmlen);
}
```