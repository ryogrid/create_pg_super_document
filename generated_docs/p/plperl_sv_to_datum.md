# plperl_sv_to_datum

## Location
[src/pl/plperl/plperl.c:1323-1443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1323-L1443)

## Overview
The primary function for converting Perl Scalar Values (SV) to PostgreSQL Datums, handling all major Perl data types including scalars, arrays, hashes, and references.

## Definition
static Datum plperl_sv_to_datum(SV *sv, Oid typid, int32 typmod, FunctionCallInfo fcinfo, FmgrInfo *finfo, Oid typioparam, bool *isnull)

## Detailed Description
This is the central conversion function in PL/Perl that transforms Perl data structures into PostgreSQL datums. It implements a comprehensive type dispatch system that handles:

1. **NULL/Undefined Values**: Converts Perl undef and NULL SVs to PostgreSQL NULL values
2. **VOID Types**: Special handling for functions returning VOID
3. **Transform Functions**: Uses registered transform functions when available for custom type conversions
4. **Array References**: Delegates to plperl_array_to_datum for array conversion
5. **Hash References**: Converts Perl hashes to PostgreSQL composite types/records
6. **Other References**: Recursively dereferences other reference types
7. **Scalar Values**: Converts strings and numbers using PostgreSQL input functions

The function includes sophisticated record type handling that can resolve RECORD types at runtime using function call information. It also performs domain validation when converting to domain types.

## Parameters / Member Variables
- sv: Perl Scalar Value to convert (may be NULL)
- typid: PostgreSQL OID of target type
- typmod: Type modifier for the target type  
- fcinfo: Function call info (for RECORD type resolution, may be NULL)
- finfo: Pre-computed function manager info (may be NULL for auto-lookup)
- typioparam: Type-specific I/O parameter (may be InvalidOid for auto-lookup)
- isnull: Output parameter indicating if result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (prevents stack overflow in recursion)
  - [_sv_to_datum_finfo](../s/_sv_to_datum_finfo.md) (sets up type conversion functions)
  - [InputFunctionCall](../I/InputFunctionCall.md) (calls PostgreSQL input functions)
  - [get_transform_tosql](../g/get_transform_tosql.md) (finds custom transform functions)
  - OidFunctionCall1 (calls transform functions)
  - [get_perl_array_ref](../g/get_perl_array_ref.md) (extracts array references)
  - [plperl_array_to_datum](plperl_array_to_datum.md) (converts arrays)
  - [type_is_rowtype](../t/type_is_rowtype.md) (checks for composite types)
  - [lookup_rowtype_tupdesc_domain](../l/lookup_rowtype_tupdesc_domain.md) (gets composite type descriptors)
  - [get_call_result_type](../g/get_call_result_type.md) (resolves RECORD types)
  - [plperl_hash_to_datum](plperl_hash_to_datum.md) (converts hashes to composite types)
  - [domain_check](../d/domain_check.md) (validates domain constraints)
  - [sv2cstr](../s/sv2cstr.md) (converts Perl strings to C strings)
- Called from (representative examples):
  - [plperl_build_tuple_result](plperl_build_tuple_result.md)
  - [array_to_datum_internal](../a/array_to_datum_internal.md)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (recursive self-call)
  - [plperl_func_handler](plperl_func_handler.md)
  - [plperl_spi_exec_prepared](plperl_spi_exec_prepared.md)

## Notes and Other Information
- Recursive function that calls check_stack_depth to prevent stack overflow
- Handles domain types by performing constraint validation after conversion
- Supports runtime RECORD type resolution using function call context
- Memory management uses appropriate PostgreSQL memory contexts
- Always sets the isnull output parameter to indicate NULL results
- Supports extensibility through transform functions for custom types
- Central hub for all PL/Perl to PostgreSQL data conversion