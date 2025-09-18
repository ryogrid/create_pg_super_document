# OpclassIsVisible

## Location
src/backend/catalog/namespace.c: 2154 - 2165

## Overview
Determines whether an operator class (identified by OID) is visible in the current search path, meaning it would be found by searching for the unqualified operator class name.

## Definition


## Detailed Description
OpclassIsVisible is a convenience function that checks if a given operator class is visible in the current PostgreSQL search path. It serves as a simple wrapper around OpclassIsVisibleExt, providing the standard visibility check without error handling for missing operator classes. The function is part of PostgreSQL's namespace resolution system, which determines which objects are accessible when referenced by unqualified names.

The visibility check is crucial for PostgreSQL's schema system, as it determines whether an operator class can be found through normal name resolution. An operator class is considered visible if it would be the first match when searching for its name through the current search path.

## Parameters / Member Variables
- `opcid`: The OID (Object Identifier) of the operator class to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - OpclassIsVisibleExt
- Called from (representative examples):
  - getObjectDescription (src/backend/catalog/objectaddress.c:3181)
  - get_opclass_name (src/backend/utils/adt/ruleutils.c:12549)

## Notes and Other Information
- This function is a thin wrapper that calls OpclassIsVisibleExt with NULL as the second parameter
- Located in src/backend/catalog/namespace.c:2154-2165
- Part of PostgreSQL's namespace visibility system used throughout the catalog management
- Returns true if the operator class is visible, false otherwise
- Does not handle missing operator classes gracefully - use OpclassIsVisibleExt for error handling