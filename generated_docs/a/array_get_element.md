# array_get_element

## Location
src/backend/utils/adt/arrayfuncs.c: 1820 - 1920

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
  - `ArrayGetOffset`: Calculates linear offset from subscripts
  - `array_get_isnull`: Checks if specific element is NULL
  - `array_seek`: Seeks to element position in array data
  - `ArrayCast`: Casts array element data to Datum
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