# enum_cmp_internal

## Location
src/backend/utils/adt/enum.c: 252 - 305

## Overview
Internal comparison engine for PostgreSQL enum values that handles the core logic for comparing two enum OIDs, with optimizations for common cases and proper handling of enum type metadata.

## Definition


## Detailed Description
The function serves as the common comparison engine for all visible enum comparison functions (except enum_eq and enum_ne which can directly compare OIDs). It implements a three-tier comparison strategy:

1. **Fast equality check**: Returns 0 immediately if both OIDs are equal
2. **Even-numbered OID optimization**: For even-numbered OIDs (which have known correct ordering), performs direct numeric comparison without consulting metadata
3. **Full metadata lookup**: For odd-numbered OIDs or mixed cases, looks up the enum type information and delegates to compare_values_of_enum()

The function uses caching via fcinfo->flinfo->fn_extra to avoid repeated type cache lookups for the same enum type.

## Parameters / Member Variables
- : First enum value OID to compare
- : Second enum value OID to compare  
- : Function call information structure containing metadata and caching context

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCallInfo (structure type)
  - Form_pg_enum (structure type)
  - lookup_type_cache
  - compare_values_of_enum
  - SearchSysCache1, HeapTupleIsValid, ReleaseSysCache (system catalog access)
- Called from:
  - enum_lt
  - enum_le
  - enum_ge
  - enum_gt
  - enum_smaller
  - enum_larger
  - enum_cmp

## Notes and Other Information
- The function contains an important optimization: even-numbered OIDs are assumed to have correct relative ordering, allowing direct numeric comparison
- Includes assertion checking to ensure fcinfo->flinfo is available even when taking fast-path exits
- Error handling for invalid enum OIDs with appropriate error codes
- Uses PostgreSQL's type cache system for efficient metadata lookup and caching