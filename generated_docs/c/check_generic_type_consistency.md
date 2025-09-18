# check_generic_type_consistency

## Location
src/backend/parser/parse_coerce.c: 1739 - 2132

## Overview
Validates that actual argument types are potentially compatible with a polymorphic function's declared argument types, enforcing PostgreSQL's complex polymorphic type consistency rules.

## Definition
```c
bool check_generic_type_consistency(const Oid *actual_arg_types, const Oid *declared_arg_types, int nargs)
```

## Detailed Description
This function implements PostgreSQL's comprehensive polymorphic type consistency checking system. It validates whether actual argument types can satisfy the constraints imposed by polymorphic pseudo-types like ANYELEMENT, ANYARRAY, ANYRANGE, ANYCOMPATIBLE, etc. The function processes multiple categories of polymorphic types: traditional ANY* types that require exact matching, and newer ANYCOMPATIBLE* types that allow implicit coercion to a common supertype. It handles complex scenarios involving arrays, ranges, multiranges, enums, and their element/subtype relationships, ensuring all constraints are satisfied before allowing function calls to proceed.

## Parameters / Member Variables
- `actual_arg_types`: Array of actual argument type OIDs being passed to the function
- `declared_arg_types`: Array of declared parameter type OIDs from the function signature
- `nargs`: Number of arguments to check (must be ≤ FUNC_MAX_ARGS)

## Dependencies
- Functions called/Symbols referenced:
  - getBaseType (flatten domain types to base types)
  - get_element_type (extract array element types)
  - get_range_subtype (extract range subtypes)
  - get_multirange_range (extract multirange element types)
  - select_common_type_from_oids (find common supertype)
  - verify_common_type_from_oids (verify coercion feasibility)
  - type_is_array_domain (check if type is array or domain over array)
  - type_is_enum (check if type is enum)
  - FUNC_MAX_ARGS (maximum function argument limit)
  - Various polymorphic type OID constants (ANYELEMENTOID, ANYARRAYOID, etc.)

- Called from (representative examples):
  - can_coerce_type (type coercion feasibility checking)

## Notes and Other Information
- Implements 10 detailed consistency rules covering all polymorphic type combinations
- Handles traditional polymorphic types (ANY*) requiring exact type matching
- Supports newer ANYCOMPATIBLE* types allowing implicit coercion to common supertypes
- Automatically flattens domain types to their base types for comparison
- Treats UNKNOWNOID (untyped literals) as compatible with any polymorphic type
- Returns false for constraint violations without raising errors (error reporting handled by callers)
- Essential for PostgreSQL's polymorphic function resolution system
- Supports complex type hierarchies including arrays, ranges, multiranges, and enums
- Located in src/backend/parser/parse_coerce.c:1739-2132