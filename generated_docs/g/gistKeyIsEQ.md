# gistKeyIsEQ

## Location
src/backend/access/gist/gistutil.c: 280 - 294

## Overview
Tests equality between two datums for a specific index column using the column's configured equality function.

## Definition
bool gistKeyIsEQ(GISTSTATE *giststate, int attno, Datum a, Datum b)

## Detailed Description
This function compares two datums for a specific index column by calling the appropriate equality function configured for that column type. It serves as a wrapper around the column-specific equality function, handling the proper function call mechanism and collation support. The function uses the GiST support function interface, where the equality function is expected to store its boolean result through a pointer parameter rather than returning it directly.

The function is essential for various GiST operations that need to determine if two keys are equivalent, such as during split operations and index maintenance.

## Parameters / Member Variables
- `giststate`: GIST state structure containing index metadata and function pointers
- `attno`: Column number (0-based) for which equality comparison is being performed
- `a`: First datum to compare
- `b`: Second datum to compare

## Dependencies
- Functions called/Symbols referenced:
  - GISTSTATE (GiST state structure type)
  - FunctionCall3Coll (calls equality function with collation support)
  - PointerGetDatum (converts pointer to Datum for passing result location)
- Called from (representative examples):
  - gistUserPicksplit (in gistsplit.c:497)
  - gistgetadjusted (in gistutil.c:351)

## Notes and Other Information
- The function follows the GiST support function protocol where the result is returned via a pointer parameter passed to the equality function
- The result variable is initialized to suppress compiler warnings about potentially uninitialized variables
- Uses FunctionCall3Coll to support collation-aware equality comparisons for text and similar data types
- This is a lightweight wrapper that provides a consistent interface for equality testing across different column types
- The equality function used is determined by the index operator class and stored in giststate->equalFn[attno]