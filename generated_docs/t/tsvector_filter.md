# tsvector_filter

## Location
src/backend/utils/adt/tsvector_op.c: 819 - 924

## Overview
Filters a tsvector to keep only lexemes with specified weights, returning a new tsvector containing only those lexemes that have positions with the given weight values.

## Definition


## Detailed Description
The  function implements the  PostgreSQL function which processes a tsvector and an array of weight characters (A, B, C, D), keeping only those lexemes that have positional information matching the specified weights. The function creates a filtered copy of the input tsvector by examining each lexeme's positional data and retaining only positions whose weights match those in the provided weight array.

The function builds a bitmask from the input weight array where each weight character ('A'/'a', 'B'/'b', 'C'/'c', 'D'/'d') corresponds to a specific bit position. It then iterates through each lexeme in the input tsvector, checking if any of its positions have weights matching the mask. Lexemes without matching positions are excluded from the output.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - Argument 0: Input tsvector to be filtered
  - Argument 1: Array of weight characters (A, B, C, D) to filter by

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract tsvector from function arguments
  - PG_GETARG_ARRAYTYPE_P: Extract array from function arguments  
  - ARRPTR: Get pointer to WordEntry array in tsvector
  - STRPTR: Get pointer to string data in tsvector
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md): Decompose input weight array
  - [DatumGetChar](../D/DatumGetChar.md): Convert Datum to char
  - _POSVECPTR: Get pointer to position vector for a word entry
  - WEP_GETWEIGHT: Extract weight from a word entry position
  - SHORTALIGN: Align memory addresses
  - POSDATALEN: Calculate position data length
  - CALCDATASIZE: Calculate total data size for tsvector
  - SET_VARSIZE: Set variable-length type size
  - PG_RETURN_POINTER: Return result pointer

- Called from (representative examples):
  - No direct callers found (exposed as PostgreSQL SQL function)

## Notes and Other Information
- The function only processes lexemes that have positional information ( is true)
- Weight characters are case-insensitive (both uppercase and lowercase accepted)
- Invalid weight characters cause an error with ERRCODE_INVALID_PARAMETER_VALUE
- NULL values in the weight array are not allowed and trigger ERRCODE_NULL_VALUE_NOT_ALLOWED
- The output tsvector is allocated with the same initial size as input but may be smaller after filtering
- Memory is realigned and compacted in the final result to minimize storage space
- Uses bitmask operations for efficient weight matching (A=8, B=4, C=2, D=1)