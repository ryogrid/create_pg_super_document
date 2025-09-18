# TypenameGetTypidExtended

## Location
src/backend/catalog/namespace.c: 1008 - 1039

## Overview
Attempts to resolve an unqualified datatype name by searching through the current namespace search path.

## Definition
Oid TypenameGetTypidExtended(const char *typname, bool temp_ok)

## Detailed Description
TypenameGetTypidExtended searches for a type by name through all namespaces in the current search path. It iterates through each namespace in activeSearchPath and uses the system cache to look up the type. The function provides control over whether to include temporary namespaces in the search. If the type is found, it returns the type's OID; if not found in any namespace in the path, it returns InvalidOid. This function is essentially the type equivalent of RelnameGetRelid for relations.

## Parameters / Member Variables
- : The name of the datatype to look up
- : Boolean flag indicating whether to search in temporary namespaces; if false, temporary namespaces are skipped

## Dependencies
- Functions called/Symbols referenced:
  - recomputeNamespacePath
  - GetSysCacheOid2
- Called from (representative examples):
  - TypenameGetTypid (src/backend/catalog/namespace.c:997)
  - LookupTypeNameExtended (src/backend/parser/parse_type.c:192)
  - RangeVarGetRelid (src/include/catalog/namespace.h:97)

## Notes and Other Information
This function follows the same pattern as other namespace resolution functions in PostgreSQL. It ensures the namespace search path is current by calling recomputeNamespacePath() first. The temp_ok parameter allows callers to control whether temporary types should be considered, which is important for certain contexts where temporary objects should be excluded. The function uses TYPENAMENSP cache for efficient lookups.