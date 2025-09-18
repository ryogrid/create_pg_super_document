# TSDictionaryIsVisible

## Location
src/backend/catalog/namespace.c: 2919 - 2930

## Overview
Determines whether a text search dictionary (identified by OID) is visible in the current search path.

## Definition
```c
bool TSDictionaryIsVisible(Oid dictId)
```

## Detailed Description
This function checks if a text search dictionary is visible in the current search path, meaning it would be found when searching for the unqualified dictionary name. The function serves as a simple wrapper around TSDictionaryIsVisibleExt, passing NULL as the second parameter to use the current active search path for standard visibility checking.

Visibility in PostgreSQL's namespace system determines whether an object can be referenced by its unqualified name during name resolution. This is crucial for determining which dictionaries are accessible without explicit schema qualification in text search operations.

## Parameters
- `dictId`: The OID of the text search dictionary to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [TSDictionaryIsVisibleExt](TSDictionaryIsVisibleExt.md)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md)
  - [regdictionaryout](../r/regdictionaryout.md)
  - RangeVarGetRelid

## Notes and Other Information
- Simple wrapper function that delegates to TSDictionaryIsVisibleExt with NULL missing parameter
- Part of PostgreSQL's visibility checking infrastructure for text search dictionary objects
- Returns true if the dictionary would be found by unqualified name lookup, false otherwise
- Mirrors the functionality of TSParserIsVisible but for dictionary objects
- Used in object description generation and regtype output formatting
- Located in src/backend/catalog/namespace.c at lines 2919-2930