# create_array_envelope

## Location
src/backend/utils/adt/arrayfuncs.c: 6056 - 6072

## Overview
A static utility function that creates the structural envelope (header) of a PostgreSQL ArrayType, initializing metadata and dimension information but not the actual data contents.

## Definition


## Detailed Description
This internal utility function constructs the basic structure of a PostgreSQL ArrayType without initializing the actual element data. It allocates memory for the complete array structure and sets up all the metadata fields including dimensions, lower bounds, element type, and data offset information.

The function serves as a building block for array construction routines, providing a clean separation between array structure setup and data population. It uses `palloc0` to ensure the allocated memory is zero-initialized, which is important for proper array structure integrity.

## Parameters / Member Variables
- `ndims`: Number of dimensions in the array
- `dimv`: Pointer to array of dimension sizes (length `ndims`)
- `lbsv`: Pointer to array of lower bounds for each dimension (length `ndims`)
- `nbytes`: Total size in bytes for the complete array structure including data
- `elmtype`: OID of the element type stored in the array
- `dataoffset`: Offset from start of ArrayType to beginning of actual data

Return value:
- `result`: Pointer to newly allocated and initialized ArrayType structure

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - SET_VARSIZE (macro to set the variable-length object size)
  - ARR_DIMS (macro to access dimension array in ArrayType)
  - ARR_LBOUND (macro to access lower bounds array in ArrayType)
  - memcpy (copies dimension and lower bound arrays)
- Called from (representative examples):
  - [array_fill_internal](../a/array_fill_internal.md) (twice, for different array construction scenarios)

## Notes and Other Information
- This is a static function, only accessible within arrayfuncs.c
- Uses zero-initialization (`palloc0`) which is critical for array structure correctness
- Separates structure creation from data population for cleaner code organization
- The `dataoffset` parameter supports arrays with custom alignment requirements
- Directly manipulates ArrayType structure fields for optimal performance
- Located in src/backend/utils/adt/arrayfuncs.c at lines 6056-6072
- Part of PostgreSQL's internal array construction infrastructure
- Assumes input parameters are valid (no validation performed at this level)
- Memory layout follows PostgreSQL's variable-length object conventions