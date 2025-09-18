# TypenameGetTypid

## Location
[src/backend/catalog/namespace.c:995-1007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L995-L1007)

## Overview
Wrapper function for binary compatibility that retrieves the OID of a type given its name.

## Definition
Oid TypenameGetTypid(const char *typname)

## Detailed Description
TypenameGetTypid is a simple wrapper function that provides backward binary compatibility for code that needs to look up a type OID by name. It internally calls TypenameGetTypidExtended with the second parameter set to true, which means it will raise an error if the type is not found rather than returning InvalidOid.

## Parameters / Member Variables
- : The name of the type to look up

## Dependencies
- Functions called/Symbols referenced:
  - [TypenameGetTypidExtended](TypenameGetTypidExtended.md)
- Called from (representative examples):
  - RangeVarGetRelid (src/include/catalog/namespace.h:96)

## Notes and Other Information
This function exists primarily for maintaining binary compatibility with existing code. New code should generally use TypenameGetTypidExtended directly to have more control over error handling behavior. The function is defined in src/backend/catalog/namespace.c:995-1007.