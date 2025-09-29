# array_get_element_expanded

## Location
[src/backend/utils/adt/arrayfuncs.c:1921-2029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1921-L2029)

## Overview
Specialized implementation of array element access for expanded arrays, providing optimized access to deconstructed array data.

## Definition
```c
static Datum array_get_element_expanded(Datum arraydatum,
                                       int nSubscripts, int *indx,
                                       int arraytyplen,
                                       int elmlen, bool elmbyval, char elmalign,
                                       bool *isNull)
```

## Detailed Description
The `array_get_element_expanded` function is a static helper function that handles element access specifically for expanded arrays. Expanded arrays are a PostgreSQL optimization where arrays are deconstructed into separate datum and null arrays for faster access. This function works with the `ExpandedArrayHeader` structure, performing sanity checks against the caller's type information, bounds checking subscripts, and accessing elements directly from the deconstructed `dvalues` and `dnulls` arrays. It automatically triggers array deconstruction if not already performed and can safely return pass-by-reference values as pointers into the expanded array structure.

## Parameters / Member Variables
- `arraydatum`: The expanded array object datum
- `nSubscripts`: Number of subscripts supplied in the indx array
- `indx[]`: Array of subscript values for each dimension
- `arraytyplen`: pg_type.typlen for the array type (should be -1 for varlena)
- `elmlen`: pg_type.typlen for the array's element type
- `elmbyval`: pg_type.typbyval for the array's element type
- `elmalign`: pg_type.typalign for the array's element type
- `*isNull`: Output parameter set to indicate whether the element is NULL

## Dependencies
- Functions called/Symbols referenced:
  - `[DatumGetEOHP](../D/DatumGetEOHP.md)`: Extracts expanded object header pointer
  - `ExpandedArrayHeader`: Expanded array header structure
  - `EA_MAGIC`: Magic number for expanded array validation
  - `[ArrayGetOffset](../A/ArrayGetOffset.md)`: Calculates linear offset from subscripts
  - `[deconstruct_expanded_array](../d/deconstruct_expanded_array.md)`: Ensures array is deconstructed into dvalues/dnulls
  - `MAXDIM`: Maximum number of array dimensions
- Called from (representative examples):
  - [array_get_element](array_get_element.md): Main array element access function for expanded arrays

## Notes and Other Information
- Static function, only accessible within `arrayfuncs.c`
- Includes comprehensive Assert statements for debugging type consistency
- Automatically handles array deconstruction on demand, even for nominally read-only inputs
- Returns direct access to `dvalues[offset]` for efficient element retrieval
- Safe to return pass-by-reference values as the expanded array structure maintains data stability
- Part of PostgreSQL's expanded object infrastructure for performance optimization
- Located in `src/backend/utils/adt/arrayfuncs.c` at lines 1921-2029

## Simplified Source

```c
static Datum array_get_element_expanded(Datum arraydatum,
                                       int nSubscripts, int *indx,
                                       int arraytyplen,
                                       int elmlen, bool elmbyval, char elmalign,
                                       bool *isNull) {
    ExpandedArrayHeader *eah;
    int i, ndim, *dim, *lb, offset;
    Datum *dvalues;
    bool *dnulls;

    // Extract expanded array header
    eah = (ExpandedArrayHeader *) DatumGetEOHP(arraydatum);
    Assert(eah->ea_magic == EA_MAGIC);

    // Verify type consistency
    Assert(arraytyplen == -1);
    Assert(elmlen == eah->typlen);
    Assert(elmbyval == eah->typbyval);
    Assert(elmalign == eah->typalign);

    // Get array dimensions and bounds
    ndim = eah->ndims;
    dim = eah->dims;
    lb = eah->lbound;

    // Validate subscripts
    if (ndim != nSubscripts || ndim <= 0 || ndim > MAXDIM) {
        *isNull = true;
        return (Datum) 0;
    }

    // Check bounds for each dimension
    for (i = 0; i < ndim; i++) {
        if (indx[i] < lb[i] || indx[i] >= (dim[i] + lb[i])) {
            *isNull = true;
            return (Datum) 0;
        }
    }

    // Calculate linear offset from subscripts
    offset = ArrayGetOffset(nSubscripts, dim, lb, indx);

    // Ensure array is deconstructed
    deconstruct_expanded_array(eah);

    dvalues = eah->dvalues;
    dnulls = eah->dnulls;

    // Check for NULL element
    if (dnulls && dnulls[offset]) {
        *isNull = true;
        return (Datum) 0;
    }

    // Return the element value
    *isNull = false;
    return dvalues[offset];
}
```