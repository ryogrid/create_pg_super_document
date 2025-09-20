# plperl_hash_from_tuple

## Location
[src/pl/plperl/plperl.c:3026-3105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3026-L3105)

## Overview
Converts a PostgreSQL tuple into a Perl hash reference, mapping column names to their corresponding values for use in PL/Perl functions.

## Definition

```c
static SV  *
plperl_hash_from_tuple(HeapTuple tuple, TupleDesc tupdesc, bool include_generated)
```
## Detailed Description
This function builds a Perl hash from all attributes of a given PostgreSQL tuple. It iterates through each attribute in the tuple descriptor, extracting values and converting them to appropriate Perl scalar values (SV). The function handles various PostgreSQL data types including:
- NULL values (converted to Perl undef)
- Row types (recursively converted to nested hashes)
- Array types (converted to Perl array references)
- Types with custom transform functions
- Standard types (converted to strings via output functions)

The function pre-allocates the hash size for efficiency and includes stack depth checking to prevent overflow during recursive calls.

## Parameters / Member Variables
- : The PostgreSQL HeapTuple containing the data to convert
- : Tuple descriptor describing the structure and types of the tuple
- : Boolean flag indicating whether to include generated columns in the output hash

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - check_stack_depth
  - [heap_getattr](../h/heap_getattr.md)
  - [hv_store_string](../h/hv_store_string.md)
  - [type_is_rowtype](../t/type_is_rowtype.md)
  - [plperl_hash_from_datum](plperl_hash_from_datum.md)
  - [get_base_element_type](../g/get_base_element_type.md)
  - [plperl_ref_from_pg_array](plperl_ref_from_pg_array.md)
  - [get_transform_fromsql](../g/get_transform_fromsql.md)
  - OidFunctionCall1
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - [cstr2sv](../c/cstr2sv.md)
  - newRV_noinc
- Called from (representative examples):
  - [plperl_trigger_build_args](plperl_trigger_build_args.md)
  - [plperl_hash_from_datum](plperl_hash_from_datum.md)
  - [plperl_spi_execute_fetch_result](plperl_spi_execute_fetch_result.md)
  - [plperl_spi_fetchrow](plperl_spi_fetchrow.md)

## Notes and Other Information
- Skips dropped columns (attisdropped) automatically
- Generated columns are only included when include_generated parameter is true
- Uses recursive calls for nested row types, protected by stack depth checking
- Efficiently pre-grows the hash using hv_ksplit for better performance
- Handles type transforms for custom data type conversions between PostgreSQL and Perl
- Returns a new Perl reference that doesn't increment the reference count of the underlying hash