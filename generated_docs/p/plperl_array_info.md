# plperl_array_info

## Location
src/pl/plperl/plperl.c: 208 - 217

## Overview
A structure that holds information necessary for converting PostgreSQL arrays to Perl data structures and vice versa in the PL/Perl procedural language extension.

## Definition


## Detailed Description
The  structure contains all the metadata and data required for bidirectional conversion between PostgreSQL arrays and Perl array references. This structure is crucial for the data type mapping functionality in PL/Perl, handling complex scenarios including multi-dimensional arrays, null values, and different element types including row types. It encapsulates both the array data and the function information needed to properly transform elements between PostgreSQL and Perl representations.

## Parameters / Member Variables
- : Number of dimensions in the array (supports multi-dimensional arrays)
- : Boolean flag indicating whether the array elements are row types (composite types) requiring special handling
- : Array of Datum values representing the actual array elements in PostgreSQL's internal format
- : Array of boolean flags indicating which elements are NULL values
- : Array of integers specifying the number of elements in each dimension
- : Function manager info structure for the primary conversion function between PostgreSQL and Perl formats
- : Function manager info structure for additional transformation procedures when needed

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references in the structure definition)
- Called from (representative examples):
  - [plperl_ref_from_pg_array](plperl_ref_from_pg_array.md) (converts PostgreSQL arrays to Perl references)
  - [split_array](../s/split_array.md) (processes array structure)
  - [make_array_ref](../m/make_array_ref.md) (creates Perl array references)

## Notes and Other Information
- This structure is essential for handling PostgreSQL's complex array types in PL/Perl functions
- The separation of  and  allows for flexible conversion pipelines when dealing with complex data types
- Support for multi-dimensional arrays through  and  makes it suitable for advanced array operations
- The  flag enables special handling for arrays containing composite types or row types
- Memory management for the dynamic arrays (, , ) is handled by the calling functions