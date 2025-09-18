# make_array_ref

## Location
[src/pl/plperl/plperl.c:1593-1630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1593-L1630)

## Overview
Creates a Perl array reference from a one-dimensional PostgreSQL array slice, converting elements to appropriate Perl values.

## Definition
static SV *make_array_ref(plperl_array_info *info, int first, int last)

## Detailed Description
This function serves as the base case for array conversion, handling the actual transformation of PostgreSQL array elements into Perl scalar values. It processes a range of elements from a flattened array, applying the appropriate conversion strategy for each element based on its type and null status. The function supports three conversion modes: transform functions (for types with custom PL/Perl transforms), composite type conversion (converting PostgreSQL records to Perl hash references), and standard output function conversion (converting to string representations). NULL values are properly handled by creating undefined Perl scalars. The result is a standard Perl array reference containing the converted elements.

## Parameters / Member Variables
- `info`: Structure containing array metadata, element data, null flags, and conversion function information
- `first`: Starting index in the element array for this slice
- `last`: Ending index (exclusive) in the element array for this slice

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall1
  - [plperl_hash_from_datum](../p/plperl_hash_from_datum.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [cstr2sv](../c/cstr2sv.md)
  - newRV_noinc
- Called from (representative examples):
  - [split_array](../s/split_array.md)

## Notes and Other Information
- Handles NULL values correctly by creating new undefined SV instead of using PL_sv_undef
- Uses transform functions when available for custom type conversions
- Automatically detects and handles composite types by converting them to hash references
- Falls back to string conversion using output functions for basic types
- Creates a proper Perl array reference with reference counting
- Does not perform bounds checking - relies on caller to provide valid indices
- Part of the recursive array conversion system used by plperl_ref_from_pg_array