# TypeIsVisible

## Location
src/backend/catalog/namespace.c: 1040 - 1051

## Overview
Determines whether a type (identified by OID) is visible in the current search path, meaning it would be found by searching for the unqualified type name.

## Definition
bool TypeIsVisible(Oid typid)

## Detailed Description
TypeIsVisible is a convenience wrapper function that checks if a type is visible in the current namespace search path. It internally calls TypeIsVisibleExt with a NULL second parameter to perform the actual visibility check. A type is considered "visible" if it would be found when searching for the unqualified type name using the current search_path setting. This is the type equivalent of RelationIsVisible for relations.

## Parameters / Member Variables
- : The OID of the type to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - TypeIsVisibleExt
- Called from (representative examples):
  - format_type_extended (src/backend/utils/adt/format_type.c:315)
  - RangeVarGetRelid (src/include/catalog/namespace.h:98)

## Notes and Other Information
This function is a simple wrapper that provides backward compatibility and a simpler interface when the extended functionality of TypeIsVisibleExt is not needed. The function follows the same pattern as RelationIsVisible for relations. It is defined in src/backend/catalog/namespace.c:1040-1051.