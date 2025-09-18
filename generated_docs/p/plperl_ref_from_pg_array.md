# plperl_ref_from_pg_array

## Location
src/pl/plperl/plperl.c: 1480 - 1558

## Overview
Converts a PostgreSQL array datum to a Perl array reference, handling multi-dimensional arrays and preserving type information.

## Definition
static SV *plperl_ref_from_pg_array(Datum arg, Oid typid)

## Detailed Description
This function transforms PostgreSQL array data into Perl data structures that can be manipulated in PL/Perl code. It handles arrays of any dimension and element type, preserving both the array structure and type metadata. The function decomposes the PostgreSQL array using the array API, extracts individual elements, and recursively builds a corresponding Perl array structure. The result is wrapped in a blessed Perl object that includes both the array data and the original PostgreSQL type OID for potential round-trip conversions. Element conversion is handled through either transform functions (if available) or standard output functions.

## Parameters / Member Variables
- `arg`: PostgreSQL Datum containing the array data to be converted
- `typid`: PostgreSQL type OID of the array type (must be an array type)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ARR_ELEMTYPE
  - ARR_NDIM
  - ARR_DIMS
  - get_type_io_data
  - get_transform_fromsql
  - fmgr_info
  - type_is_rowtype
  - deconstruct_array
  - split_array
  - newRV_noinc
  - newSVuv
- Called from (representative examples):
  - plperl_call_perl_func
  - plperl_hash_from_tuple

## Notes and Other Information
- Returns a blessed Perl object of class "PostgreSQL::InServer::ARRAY"
- The returned object contains both "array" and "typeoid" keys for complete type preservation
- Handles empty arrays (zero dimensions) by returning an empty Perl array reference
- Uses transform functions when available for element conversion, falling back to output functions
- Currently does not cache type information lookups, which could impact performance
- Supports multi-dimensional arrays through recursive splitting via split_array function
- Memory allocation uses PostgreSQL memory context for intermediate data structures