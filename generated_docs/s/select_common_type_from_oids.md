# select_common_type_from_oids

## Location
src/backend/parser/parse_coerce.c: 1480 - 1573

## Overview
Determines the common supertype from an array of type OIDs using the same logic as select_common_type, but working directly with type identifiers rather than expression nodes.

## Definition
```c
static Oid select_common_type_from_oids(int nargs, const Oid *typeids, bool noerror)
```

## Detailed Description
This function implements the same type resolution algorithm as `select_common_type` but operates on an array of type OIDs instead of expression nodes. It follows identical logic for determining type compatibility and preference:

1. **Exact Match Check**: If all type OIDs are identical, that type is returned immediately.

2. **Base Type Conversion**: Domain types are converted to their base types for comparison.

3. **Category Analysis**: Types are checked for category compatibility, with different categories causing failure.

4. **Preference Resolution**: Within the same category, preferred types are favored, and implicit coercibility determines selection between non-preferred types.

5. **Unknown Type Handling**: If all inputs are UNKNOWN, TEXT is selected as the default.

The function is primarily used internally for generic type consistency checking in polymorphic functions, where type OIDs are already available rather than needing to extract them from expressions.

## Parameters / Member Variables
- `nargs`: Number of type OIDs in the array (must be > 0)
- `typeids`: Array of type OIDs to find common type for
- `noerror`: If true, return InvalidOid on failure; if false, throw an error

## Dependencies
- Functions called/Symbols referenced:
  - getBaseType
  - get_type_category_preferred  
  - can_coerce_type
  - format_type_be
  - ereport (for error handling)
  - COERCION_IMPLICIT (constant)
  - TYPCATEGORY (type)
- Called from (representative examples):
  - check_generic_type_consistency (src/backend/parser/parse_coerce.c:2002)
  - enforce_generic_type_consistency (src/backend/parser/parse_coerce.c:2586)

## Notes and Other Information
- Marked as static - internal function not exposed outside parse_coerce.c
- Earlier entries in the type array have preference over later ones, matching `select_common_type` behavior
- Contains logic for UNKNOWNOID handling that is currently dead code since callers don't pass UNKNOWN types, but maintained for consistency with `select_common_type`
- Used specifically for polymorphic function type resolution where consistency between generic types (anyarray, anyelement, etc.) must be enforced
- Like its expression-based counterpart, determines compatibility but doesn't guarantee coercibility - callers should verify with `verify_common_type_from_oids`