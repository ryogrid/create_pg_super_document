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