# split_array

## Location
src/pl/plperl/plperl.c: 1559 - 1592

## Overview
Recursively converts multi-dimensional PostgreSQL array data into nested Perl array references by splitting array dimensions.

## Definition
static SV *split_array(plperl_array_info *info, int first, int last, int nest)

## Detailed Description
This function implements the recursive logic for transforming multi-dimensional PostgreSQL arrays into corresponding nested Perl array structures. It works by partitioning the flattened array elements according to dimension boundaries and recursively creating array references for each sub-dimension. The base case occurs when processing the innermost dimension, where it delegates to make_array_ref to create a simple one-dimensional array. For higher dimensions, it creates a new Perl array and populates it with references returned from recursive calls to lower dimensions. The function includes stack depth checking to prevent stack overflow during deep recursion.

## Parameters / Member Variables
- `info`: Structure containing array metadata including dimensions, element data, and conversion functions
- `first`: Starting index in the flattened element array for this dimensional slice
- `last`: Ending index (exclusive) in the flattened element array for this dimensional slice
- `nest`: Current nesting level (dimension index) being processed

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - make_array_ref
  - split_array (recursive call)
  - newRV_noinc
- Called from (representative examples):
  - plperl_ref_from_pg_array
  - split_array (recursive self-call)

## Notes and Other Information
- Requires that info->ndims > 0 (enforced by assertion)
- Uses tail recursion optimization opportunity when processing nested dimensions
- Stack depth checking prevents infinite recursion and stack overflow
- Base case delegation to make_array_ref handles the actual element conversion
- Returns blessed Perl array references that maintain the original dimensional structure
- Memory management relies on Perl reference counting for the created structures