# enforce_generic_type_consistency

## Location
src/backend/parser/parse_coerce.c: 2133 - 2876

## Overview
Enforces type consistency rules for polymorphic functions and deduces actual argument and result types from polymorphic pseudotypes.

## Definition


## Detailed Description
This function ensures that polymorphic functions are legally callable by enforcing consistency rules among polymorphic arguments and deducing concrete types. It handles two families of polymorphic types:

**Family-1 polymorphic types** (ANYELEMENT, ANYARRAY, ANYRANGE, etc.):
- All arguments of the same polymorphic type must resolve to the same concrete type
- Element types must be consistent across ANYELEMENT, ANYARRAY, and ANYRANGE arguments
- Special constraints for ANYENUM (must be enum type) and ANYNONARRAY (must not be array)

**Family-2 polymorphic types** (ANYCOMPATIBLE family):
- Arguments are resolved to a common supertype that all can be cast to
- Includes ANYCOMPATIBLE, ANYCOMPATIBLEARRAY, ANYCOMPATIBLERANGE, etc.
- Range and multirange types must have exact subtype matching

The function also handles UNKNOWN input literals by deducing their appropriate types and updates the declared_arg_types array accordingly for proper coercion.

## Parameters / Member Variables
- : Array of actual argument type OIDs passed to the function
- : Array of declared argument type OIDs (may be modified for UNKNOWN inputs)
- : Number of function arguments
- : Declared return type OID of the function
- : Whether polymorphic types are allowed in actual arguments and return type

## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md)
  - [get_element_type](../g/get_element_type.md)
  - [get_range_subtype](../g/get_range_subtype.md)
  - [get_multirange_range](../g/get_multirange_range.md)
  - [get_range_multirange](../g/get_range_multirange.md)
  - [get_array_type](../g/get_array_type.md)
  - [select_common_type_from_oids](../s/select_common_type_from_oids.md)
  - [verify_common_type_from_oids](../v/verify_common_type_from_oids.md)
  - IsPolymorphicTypeFamily1
  - type_is_array_domain
  - [type_is_enum](../t/type_is_enum.md)
  - FUNC_MAX_ARGS
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [lookup_agg_function](../l/lookup_agg_function.md)
  - [make_op](../m/make_op.md)
  - [make_scalar_array_op](../m/make_scalar_array_op.md)

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2133-2876
- Critical for PostgreSQL's polymorphic type system functionality
- Handles complex type resolution scenarios including domain flattening
- Special handling for pg_statistic columns that appear as anyarray
- Extensive error checking with detailed error messages for type mismatches
- Returns the resolved return type OID or original rettype if no polymorphic arguments