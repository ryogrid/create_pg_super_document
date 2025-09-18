# resolve_anyrange_from_others

## Location
src/backend/utils/fmgr/funcapi.c: 681 - 709

## Overview
Resolves the actual type of ANYRANGE polymorphic type parameter from other polymorphic inputs, specifically from ANYMULTIRANGE type when available.

## Definition
static void resolve_anyrange_from_others(polymorphic_actuals *actuals)

## Detailed Description
This function resolves the ANYRANGE polymorphic type with a key limitation: it can only deduce the range type from a polymorphic multirange type, not from array or base element types. This is because multiple range types can share the same subtype, making it impossible to uniquely determine which range type is intended from just the element type.

When anymultirange_type is valid, the function extracts the range type that the multirange contains using get_multirange_range(). This provides a unique mapping from multirange to range type.

## Parameters / Member Variables
- : Pointer to polymorphic_actuals structure containing resolved and unresolved polymorphic type OIDs. The function reads anymultirange_type and sets anyrange_type.

## Dependencies
- Functions called/Symbols referenced:
  - getBaseType: Gets the base type of a potentially domain type
  - get_multirange_range: Extracts the range type from a multirange type
  - OidIsValid: Macro to check if an OID is valid
  - ereport/elog: Error reporting functions
  - format_type_be: Formats type OID as string for error messages

- Called from (representative examples):
  - resolve_polymorphic_tupdesc: When resolving tuple descriptors with polymorphic types
  - resolve_polymorphic_argtypes: When resolving function argument types

## Notes and Other Information
- This is a static function, only used within funcapi.c
- Unlike other resolve functions, this cannot deduce range type from element or array types due to potential ambiguity
- Only works when anymultirange_type is already resolved
- The design reflects PostgreSQL's type system where multiple range types can have the same subtype
- Located in src/backend/utils/fmgr/funcapi.c:681-709