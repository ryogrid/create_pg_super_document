# StatisticsObjIsVisible

## Location
[src/backend/catalog/namespace.c:2632-2643](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2632-L2643)

## Overview
Determines whether a statistics object (identified by OID) is visible in the current search path, meaning it would be found when searching for the unqualified statistics object name.

## Definition
```c
bool StatisticsObjIsVisible(Oid stxid)
```

## Detailed Description
StatisticsObjIsVisible is a simple wrapper function that checks if a given statistics object is visible in the current search path. It delegates to StatisticsObjIsVisibleExt with a NULL parameter for the is_missing flag, which means it will throw an error if the statistics object is not found rather than returning a missing indicator.

The function serves as the primary interface for visibility checking of statistics objects when error handling for missing objects is not needed. It follows PostgreSQL's pattern of providing both simple and extended versions of visibility checking functions.

## Parameters / Member Variables
- `stxid`: OID of the statistics object to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [StatisticsObjIsVisibleExt](StatisticsObjIsVisibleExt.md)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md) (in objectaddress.c)
  - Referenced in namespace.h header

## Notes and Other Information
- This is a thin wrapper around StatisticsObjIsVisibleExt that provides simpler error semantics
- The function will throw an error if the statistics object OID is invalid, rather than gracefully handling the missing case
- Part of PostgreSQL's namespace visibility system for extended statistics objects
- Statistics objects are part of PostgreSQL's multi-column statistics feature
- Located in src/backend/catalog/namespace.c:2632-2643