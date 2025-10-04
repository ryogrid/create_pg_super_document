# array_to_datum_internal

## Location
[src/pl/plperl/plperl.c:1170-1256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1170-L1256)

## Overview
Recursively processes multidimensional Perl arrays to convert them into PostgreSQL Datum format, handling dimension validation and element conversion.

## Definition
static void array_to_datum_internal(AV *av, ArrayBuildState **astatep, int *ndims, int *dims, int cur_depth, Oid elemtypid, int32 typmod, FmgrInfo *finfo, Oid typioparam)

## Detailed Description
This is the core recursive helper function for converting Perl arrays to PostgreSQL array datums. It traverses multidimensional arrays depth-first, building the PostgreSQL array structure incrementally. The function handles several key responsibilities:

1. **Dimension Management**: Tracks and validates array dimensions, ensuring consistency across all levels
2. **Recursive Processing**: Handles nested arrays by recursively calling itself for sub-arrays
3. **Element Conversion**: Converts individual scalar elements using plperl_sv_to_datum
4. **Error Handling**: Validates array structure and reports errors for malformed arrays
5. **Memory Management**: Uses ArrayBuildState to efficiently accumulate array elements

The function creates the ArrayBuildState only when it encounters the first scalar element, which helps determine when the array dimensions are finalized.

## Parameters / Member Variables
- av: Perl AV (array value) to process
- astatep: Pointer to ArrayBuildState for accumulating results
- ndims: Pointer to number of dimensions discovered so far
- dims: Array storing the size of each dimension
- cur_depth: Current recursion depth (0-based)
- elemtypid: PostgreSQL OID of the array element type
- typmod: Type modifier for the element type
- finfo: Function manager info for element conversion
- typioparam: Type-specific parameter for conversion

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - [get_perl_array_ref](../g/get_perl_array_ref.md) (extracts array references from SVs)
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md) (converts scalar values to datums)
  - [initArrayResult](../i/initArrayResult.md) (initializes array building state)
  - [accumArrayResult](accumArrayResult.md) (adds elements to array being built)
  - MAXDIM (maximum allowed dimensions constant)
- Called from (representative examples):
  - [array_to_datum_internal](array_to_datum_internal.md) (recursive self-call)
  - [plperl_array_to_datum](../p/plperl_array_to_datum.md)

## Notes and Other Information
- Enforces PostgreSQL maximum dimension limit (MAXDIM)
- Validates that all sub-arrays at the same level have matching dimensions
- Detects and reports errors for mixed scalar/array elements at the same level
- Uses efficient lazy initialization of ArrayBuildState to avoid unnecessary work
- Handles both regular Perl arrays and PostgreSQL::InServer::ARRAY objects through get_perl_array_ref
- Memory allocation occurs in CurrentMemoryContext for proper cleanup

## Simplified Source

```c
static void
array_to_datum_internal(AV *av, ArrayBuildState **astatep,
                        int *ndims, int *dims, int cur_depth,
                        Oid elemtypid, int32 typmod,
                        FmgrInfo *finfo, Oid typioparam)
{
    int array_length = av_len(av) + 1;

    // Process each element in the array
    for (int i = 0; i < array_length; i++) {
        SV **element_ptr = av_fetch(av, i, FALSE);
        SV *sub_array = element_ptr ? get_perl_array_ref(*element_ptr) : NULL;

        if (sub_array) {
            // This element is a sub-array - handle multidimensional case
            AV *nested_array = (AV *) SvRV(sub_array);

            // Set dimension size at first element, validate consistency thereafter
            if (i == 0 && *ndims == cur_depth) {
                // Check for mixed scalars and arrays
                if (*astatep != NULL) {
                    ereport(ERROR, "arrays must have matching dimensions");
                }
                // Check dimension limit
                if (cur_depth + 1 > MAXDIM) {
                    ereport(ERROR, "too many array dimensions");
                }
                // Record this dimension size
                dims[*ndims] = av_len(nested_array) + 1;
                (*ndims)++;
            } else {
                // Validate dimension consistency
                if (cur_depth >= *ndims || av_len(nested_array) + 1 != dims[cur_depth]) {
                    ereport(ERROR, "arrays must have matching dimensions");
                }
            }

            // Recursively process the sub-array
            array_to_datum_internal(nested_array, astatep,
                                    ndims, dims, cur_depth + 1,
                                    elemtypid, typmod, finfo, typioparam);
        } else {
            // This element is a scalar value
            if (*ndims != cur_depth) {
                ereport(ERROR, "arrays must have matching dimensions");
            }

            // Convert Perl scalar to PostgreSQL datum
            bool is_null;
            Datum datum = plperl_sv_to_datum(element_ptr ? *element_ptr : NULL,
                                             elemtypid, typmod, NULL,
                                             finfo, typioparam, &is_null);

            // Initialize array builder on first scalar element
            if (*astatep == NULL) {
                *astatep = initArrayResult(elemtypid, CurrentMemoryContext, true);
            }

            // Add element to the array being built
            accumArrayResult(*astatep, datum, is_null, elemtypid, CurrentMemoryContext);
        }
    }
}
```