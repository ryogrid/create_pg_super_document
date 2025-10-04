# plperl_array_to_datum

## Location
[src/pl/plperl/plperl.c:1257-1299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1257-L1299)

## Overview
Converts a Perl array reference to a PostgreSQL array Datum, serving as the main entry point for array conversion in PL/Perl.

## Definition
static Datum plperl_array_to_datum(SV *src, Oid typid, int32 typmod)

## Detailed Description
This function is the primary interface for converting Perl array references into PostgreSQL array datums. It coordinates the entire conversion process by:

1. **Type Validation**: Verifies that the target PostgreSQL type is actually an array type
2. **Element Type Resolution**: Determines the PostgreSQL type of array elements using get_element_type
3. **Function Info Setup**: Prepares conversion functions for element types via _sv_to_datum_finfo  
4. **Dimension Initialization**: Sets up dimension tracking arrays and establishes initial bounds
5. **Recursive Processing**: Delegates the actual array traversal to array_to_datum_internal
6. **Result Construction**: Builds the final PostgreSQL array datum with proper dimensions and bounds

The function handles edge cases like empty arrays by constructing zero-dimensional arrays following PostgreSQL conventions. It uses PostgreSQL lower bounds of 1 for all dimensions, which is the standard convention.

## Parameters / Member Variables
- src: Perl SV containing the array reference to convert
- typid: PostgreSQL OID of the target array type  
- typmod: Type modifier for the target array type

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - [get_element_type](../g/get_element_type.md) (extracts element type from array type)
  - [_sv_to_datum_finfo](../s/_sv_to_datum_finfo.md) (sets up conversion function info)
  - [array_to_datum_internal](../a/array_to_datum_internal.md) (performs recursive array processing)
  - [construct_empty_array](../c/construct_empty_array.md) (creates zero-dimensional arrays)
  - [makeMdArrayResult](../m/makeMdArrayResult.md) (constructs final multidimensional array datum)
  - MAXDIM (maximum dimensions constant)
  - [ArrayBuildState](../A/ArrayBuildState.md) (array construction state type)
- Called from (representative examples):
  - [plperl_sv_to_datum](plperl_sv_to_datum.md)

## Notes and Other Information  
- Assumes input src is already validated as an array reference
- Uses PostgreSQL standard lower bound of 1 for all array dimensions
- Handles empty arrays by creating zero-dimensional arrays per PostgreSQL convention
- Memory management uses CurrentMemoryContext for proper cleanup
- Supports arrays up to MAXDIM dimensions
- Throws errors for non-array target types with descriptive messages including type names

## Simplified Source

```c
static Datum
plperl_array_to_datum(SV *src, Oid typid, int32 typmod)
{
    // Extract the Perl array from the reference
    AV *perl_array = (AV *) SvRV(src);
    ArrayBuildState *astate = NULL;

    // Validate that target type is an array and get element type
    Oid elemtypid = get_element_type(typid);
    if (!elemtypid) {
        ereport(ERROR, "cannot convert Perl array to non-array type");
    }

    // Setup conversion function info for element type
    FmgrInfo finfo;
    Oid typioparam;
    _sv_to_datum_finfo(elemtypid, &finfo, &typioparam);

    // Initialize dimension tracking
    int dims[MAXDIM];
    int lbs[MAXDIM];  // lower bounds
    int ndims = 1;

    memset(dims, 0, sizeof(dims));
    dims[0] = av_len(perl_array) + 1;  // First dimension size

    // Process the array recursively
    array_to_datum_internal(perl_array, &astate,
                            &ndims, dims, 1,
                            elemtypid, typmod,
                            &finfo, typioparam);

    // Handle empty array case
    if (astate == NULL) {
        return PointerGetDatum(construct_empty_array(elemtypid));
    }

    // Set standard PostgreSQL lower bounds (1-based indexing)
    for (int i = 0; i < ndims; i++) {
        lbs[i] = 1;
    }

    // Create the final multidimensional array
    return makeMdArrayResult(astate, ndims, dims, lbs,
                             CurrentMemoryContext, true);
}
```