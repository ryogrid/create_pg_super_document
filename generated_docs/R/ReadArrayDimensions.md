# ReadArrayDimensions

## Location
src/backend/utils/adt/arrayfuncs.c: 402 - 518

## Overview
Parses array dimension specifications from an input string and converts them to internal format, extracting bounds and dimension sizes for multi-dimensional arrays.

## Definition


## Detailed Description
ReadArrayDimensions is a static helper function that parses the optional dimension specification part of PostgreSQL array literals. It handles dimension specifications in the format "[n]" for simple dimensions or "[m:n]" for explicit lower and upper bounds.

The function processes dimension items sequentially, validating bounds and computing dimension sizes. It supports multi-dimensional arrays up to MAXDIM dimensions. The parsing follows these rules:
- Dimension items are enclosed in square brackets: [n] or [m:n]
- Whitespace is allowed between dimension items but not within them
- For [n] format, lower bound defaults to 1 and upper bound is n
- For [m:n] format, lower bound is m and upper bound is n
- Upper bound cannot be less than lower bound
- Upper bound cannot be INT_MAX (reserved for internal use)

The function performs careful overflow checking when computing dimension sizes to prevent integer overflow attacks.

## Parameters / Member Variables
- : Pointer to current position in input string, advanced during parsing
- : Output parameter for number of dimensions found
- : Output array for dimension sizes (caller-allocated, MAXDIM elements)
- : Output array for lower bounds of each dimension (caller-allocated, MAXDIM elements)
- : Original input string (used only for error messages)
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - ReadDimensionInt
  - scanner_isspace
  - pg_sub_s32_overflow
  - pg_add_s32_overflow
  - ereturn (error handling macros)
  - MAXDIM
  - MaxArraySize
- Called from (representative examples):
  - array_in

## Notes and Other Information
- Static function internal to arrayfuncs.c
- Advances the source pointer (*srcptr) to the position after parsed dimensions
- Sets *ndim_p to 0 if no dimension specifications are found
- Validates that dimensions don't exceed MAXDIM limit
- Performs overflow checking to prevent arithmetic overflow in dimension calculations
- Does not accept zero-length dimensions (where upper bound < lower bound)
- Uses soft error handling through escontext when available