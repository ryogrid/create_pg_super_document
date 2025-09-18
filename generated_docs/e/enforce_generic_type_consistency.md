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
  - getBaseType
  - get_element_type
  - get_range_subtype
  - get_multirange_range
  - get_range_multirange
  - get_array_type
  - select_common_type_from_oids
  - verify_common_type_from_oids
  - IsPolymorphicTypeFamily1
  - type_is_array_domain
  - type_is_enum
  - FUNC_MAX_ARGS
- Called from (representative examples):
  - ParseFuncOrColumn
  - lookup_agg_function
  - make_op
  - make_scalar_array_op

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2133-2876
- Critical for PostgreSQL's polymorphic type system functionality
- Handles complex type resolution scenarios including domain flattening
- Special handling for pg_statistic columns that appear as anyarray
- Extensive error checking with detailed error messages for type mismatches
- Returns the resolved return type OID or original rettype if no polymorphic arguments