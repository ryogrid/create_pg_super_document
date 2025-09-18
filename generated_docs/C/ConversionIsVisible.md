# ConversionIsVisible

## Location
[src/backend/catalog/namespace.c:2509-2520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2509-L2520)

## Overview
Determines whether a conversion (identified by OID) is visible in the current search path, meaning it would be found when searching for the unqualified conversion name.

## Definition
```c
bool ConversionIsVisible(Oid conid)
```

## Detailed Description
ConversionIsVisible is a simple wrapper function that checks if a given conversion is visible in the current search path. It delegates to ConversionIsVisibleExt with a NULL parameter for the is_missing flag, which means it will throw an error if the conversion is not found rather than returning a missing indicator.

The function serves as the primary interface for visibility checking when error handling for missing conversions is not needed. It follows PostgreSQL's pattern of providing both simple and extended versions of visibility checking functions.

## Parameters / Member Variables
- `conid`: OID of the conversion to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [ConversionIsVisibleExt](ConversionIsVisibleExt.md)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md) (in objectaddress.c)
  - Referenced in namespace.h header

## Notes and Other Information
- This is a thin wrapper around ConversionIsVisibleExt that provides simpler error semantics
- The function will throw an error if the conversion OID is invalid, rather than gracefully handling the missing case
- Part of PostgreSQL's namespace visibility system that determines which objects are accessible without schema qualification
- Located in src/backend/catalog/namespace.c:2509-2520