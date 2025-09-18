# construct_empty_array

## Location
src/backend/utils/adt/arrayfuncs.c: 3568 - 3584

## Overview
Creates a zero-dimensional empty array object of a specified element type, providing the simplest possible array structure in PostgreSQL.

## Definition


## Detailed Description
The construct_empty_array function creates the most basic form of array in PostgreSQL - a zero-dimensional array with no elements. This function is used as a building block for other array operations and as a return value when array operations result in empty collections. Unlike multi-dimensional arrays, this function creates a minimal ArrayType structure with just the basic header information and no dimension arrays, lower bound arrays, or element data.

The resulting array has ndim set to 0, dataoffset set to 0 (indicating no null bitmap), and only contains the element type information. This represents the canonical empty array in PostgreSQL's type system.

## Parameters / Member Variables
- : OID of the data type that the empty array would contain if it had elements

## Dependencies
- Functions called/Symbols referenced:
  - SET_VARSIZE
- Called from (representative examples):
  - construct_md_array
  - construct_empty_expanded_array
  - pg_identify_object_as_address
  - pg_event_trigger_dropped_objects
  - transformGenericOptions
  - ExecEvalArrayExpr
  - array_shuffle_n
  - array_in
  - array_recv
  - array_get_slice
  - array_map
  - makeArrayResultArr
  - array_fill_internal
  - array_replace_internal
  - array_subscript_assign
  - percentile_disc_multi_final
  - text_to_array
  - plperl_array_to_datum
  - PLySequence_ToArray

## Notes and Other Information
- Creates the minimal possible ArrayType structure with just basic header information
- The resulting array has zero dimensions (ndim = 0) and no data payload
- Memory allocation is exactly sizeof(ArrayType) with no additional space for elements
- This function is frequently used as a starting point or fallback for array operations that may result in empty collections
- The empty array maintains type information even though it contains no elements
- Used extensively throughout PostgreSQL's array handling code as the canonical representation of empty arrays