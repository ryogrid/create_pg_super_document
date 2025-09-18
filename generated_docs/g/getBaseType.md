# getBaseType

## Location
src/backend/utils/cache/lsyscache.c: 2521 - 2537

## Overview
Returns the base type OID for a given type, resolving domain types to their underlying base types while returning the same OID for non-domain types.

## Definition


## Detailed Description
This function provides a convenient interface for resolving PostgreSQL domain types to their underlying base types. A domain in PostgreSQL is a user-defined data type that is based on an existing type but can have additional constraints. When given a domain type OID, this function returns the OID of the underlying base type. For regular (non-domain) types, it simply returns the same type OID. Internally, it delegates to  with a dummy typmod parameter, making it a simplified wrapper for cases where the type modifier is not needed.

## Parameters / Member Variables
- : The OID of the type to resolve, which may be either a domain type or a regular type

## Dependencies
- Functions called/Symbols referenced:
  - getBaseTypeAndTypmod
- Called from (representative examples):
  - GetIndexInputType
  - find_expr_references_walker
  - CheckAttributeType
  - GetDefaultOpClass
  - ATAddForeignKeyConstraint
  - coerce_type
  - select_common_type
  - check_generic_type_consistency
  - enforce_generic_type_consistency
  - func_select_candidate
  - binary_oper_exact
  - range_typanalyze
  - type_is_rowtype

## Notes and Other Information
- This function is essential for type system operations that need to work with the underlying type rather than domain wrappers
- Commonly used in type coercion, operator resolution, and constraint checking where domain constraints should be ignored
- Part of the lsyscache.c module which provides cached access to system catalog information
- The function is heavily used throughout the parser, type system, and executor for resolving type compatibility
- Domain types inherit most properties from their base types, so this function enables treating domains transparently in many contexts
- Returns the input type OID unchanged for built-in types, user-defined base types, and composite types
- Critical for generic type resolution in polymorphic functions and operators