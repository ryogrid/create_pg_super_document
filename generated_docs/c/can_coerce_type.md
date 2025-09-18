# can_coerce_type

## Location
src/backend/parser/parse_coerce.c: 556 - 675

## Overview
Determines whether a set of input types can be coerced to corresponding target types within a given coercion context.

## Definition


## Detailed Description
This function serves as the feasibility checker for type coercion operations, validating whether type conversions are possible before attempting them. It evaluates each input-target type pair according to PostgreSQL's coercion rules:

1. **Identity Check**: Same types always coerce successfully
2. **ANY Type**: ANY pseudotype accepts any input
3. **Polymorphic Types**: Polymorphic pseudotypes are accepted with additional consistency checking
4. **Unknown Constants**: UNKNOWN type can coerce to any target type
5. **Cast System**: Consults pg_cast catalog for explicit coercion functions and binary compatibility
6. **Record Coercion**: Handles RECORD to/from complex types
7. **Array Coercion**: Manages complex array type conversions
8. **Inheritance**: Supports subclass to superclass coercion via type inheritance

The function performs polymorphic type consistency checking when generic types are involved, ensuring that all polymorphic parameters resolve to compatible actual types.

## Parameters / Member Variables
- : Number of argument types to check for coercion
- : Array of input type OIDs to be coerced from
- : Array of target type OIDs to be coerced to
- : Coercion context (CAST, assignment, implicit) determining available casts

## Dependencies
- Functions called/Symbols referenced:
  - find_coercion_pathway
  - IsPolymorphicType
  - check_generic_type_consistency
  - typeInheritsFrom
  - typeIsOfTypedTable
  - is_complex_array
  - ISCOMPLEX
  - CoercionContext (enum)
  - CoercionPathType (enum)
- Called from (representative examples):
  - coerce_to_target_type
  - select_common_type
  - func_match_argtypes
  - func_select_candidate
  - verify_common_type

## Notes and Other Information
- Returns false immediately upon finding any non-coercible type pair
- Performs polymorphic type consistency validation as a final step when generic types are present
- Does not perform actual coercion - only validates feasibility
- Essential prerequisite function called before  operations
- Supports both single and multi-argument type checking scenarios
- Contains provisions for record array coercion (currently disabled with NOT_USED)
- Located in src/backend/parser/parse_coerce.c:556-675