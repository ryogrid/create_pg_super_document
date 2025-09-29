# OpfamilyIsVisible

## Location
[src/backend/catalog/namespace.c:2256-2267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2256-L2267)

## Overview
Determines whether an operator family (identified by OID) is visible in the current search path, meaning it would be found by searching for the unqualified operator family name.

## Definition

```c
bool
OpfamilyIsVisible(Oid opfid)
```
## Detailed Description
OpfamilyIsVisible is a convenience function that checks if a given operator family is visible in the current PostgreSQL search path. It serves as a simple wrapper around OpfamilyIsVisibleExt, providing the standard visibility check without error handling for missing operator families. The function is part of PostgreSQL's namespace resolution system, which determines which objects are accessible when referenced by unqualified names.

The visibility check is essential for PostgreSQL's schema system, as it determines whether an operator family can be found through normal name resolution. An operator family is considered visible if it would be the first match when searching for its name through the current search path, taking into account the specific access method it belongs to.

## Parameters / Member Variables
- `opfid`: The OID (Object Identifier) of the operator family to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [OpfamilyIsVisibleExt](OpfamilyIsVisibleExt.md)
- Called from (representative examples):
  - [getOpFamilyDescription](../g/getOpFamilyDescription.md) (src/backend/catalog/objectaddress.c:4187)

## Notes and Other Information
- This function is a thin wrapper that calls OpfamilyIsVisibleExt with NULL as the second parameter
- Located in src/backend/catalog/namespace.c:2256-2267
- Part of PostgreSQL's namespace visibility system used throughout catalog management
- Returns true if the operator family is visible, false otherwise
- Does not handle missing operator families gracefully - use OpfamilyIsVisibleExt for error handling
- Similar in structure and purpose to OpclassIsVisible but for operator families instead of operator classes

## Simplified Source

```c
bool OpfamilyIsVisible(Oid opfid) {
    // Simple wrapper that delegates to the extended version
    // with NULL for the second parameter (no error handling)
    return OpfamilyIsVisibleExt(opfid, NULL);
}
```