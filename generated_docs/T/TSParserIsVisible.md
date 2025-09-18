# TSParserIsVisible

## Location
src/backend/catalog/namespace.c: 2774 - 2785

## Overview
Determines whether a text search parser (identified by OID) is visible in the current search path.

## Definition
```c
bool TSParserIsVisible(Oid prsId)
```

## Detailed Description
This function checks if a text search parser is visible in the current search path, meaning it would be found when searching for the unqualified parser name. The function serves as a simple wrapper around TSParserIsVisibleExt, passing NULL as the second parameter to use the current active search path.

Visibility in PostgreSQL's namespace system means that an object can be referenced by its unqualified name and would be found during name resolution. This is important for determining which objects are accessible without explicit schema qualification.

## Parameters
- `prsId`: The OID of the text search parser to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [TSParserIsVisibleExt](TSParserIsVisibleExt.md)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md)
  - RangeVarGetRelid

## Notes and Other Information
- Simple wrapper function that delegates to TSParserIsVisibleExt with NULL namespace parameter
- Part of PostgreSQL's visibility checking infrastructure for text search objects
- Returns true if the parser would be found by unqualified name lookup, false otherwise
- Located in src/backend/catalog/namespace.c at lines 2774-2785