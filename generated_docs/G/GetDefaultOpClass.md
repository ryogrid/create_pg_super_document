# GetDefaultOpClass

## Location
src/backend/commands/indexcmds.c: 2278 - 2386

## Overview
Finds the default operator class for a given data type and access method combination, handling exact matches and binary-compatible alternatives with preference rules.

## Definition


## Detailed Description
This function searches through all available operator classes for a specific access method to find the appropriate default operator class for a given data type. The search algorithm implements a sophisticated preference system:

1. **Exact Type Match**: Prioritizes operator classes that exactly match the target data type
2. **Binary Compatible Match**: Falls back to operator classes for binary-compatible types
3. **Preferred Type Resolution**: Among binary-compatible matches, prefers operator classes for "preferred" types in the same type category (e.g., text over bpchar for string types)

The function performs a full scan of the pg_opclass system catalog, filtering by access method and checking only those operator classes marked as default. It includes validation to ensure there are no duplicate default operator classes for the same type, which would indicate inconsistent catalog data.

For domain types, the function automatically resolves to the base type before performing the search, ensuring that domains inherit the default operator class behavior of their underlying type.

## Parameters / Member Variables
- : OID of the data type for which to find a default operator class
- : OID of the access method (btree, hash, gist, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - getBaseType (for domain type resolution)
  - TypeCategory (for type category determination)
  - table_open, table_close (for catalog access)
  - systable_beginscan, systable_getnext, systable_endscan (for catalog scanning)
  - IsBinaryCoercible (for type compatibility checking)
  - IsPreferredType (for preferred type determination)
  - ScanKeyInit (for search key setup)
- Called from (representative examples):
  - ResolveOpClass (when no explicit operator class is specified)
  - ComputePartitionAttrs (for partition key default operator classes)
  - lookup_type_cache (for type cache initialization)
  - transformIndexConstraint (during constraint processing)

## Notes and Other Information
- Returns InvalidOid if no suitable default operator class is found
- Handles the special case where multiple binary-compatible matches exist by preferring "preferred" types
- Raises an error if multiple exact matches are found, indicating catalog inconsistency
- Automatically handles domain types by resolving to their base types
- Critical for automatic operator class selection in index creation and partitioning
- The preference system resolves ambiguity for types like varchar that are compatible with multiple operator classes