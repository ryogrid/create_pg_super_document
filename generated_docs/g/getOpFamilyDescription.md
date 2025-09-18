# getOpFamilyDescription

## Location
src/backend/catalog/objectaddress.c: 4163 - 4204

## Overview
A specialized subroutine that generates human-readable descriptions of PostgreSQL operator families by looking up family and access method information and formatting descriptive text.

## Definition
```c
static void getOpFamilyDescription(StringInfo buffer, Oid opfid, bool missing_ok)
```

## Detailed Description
This static helper function is called by getObjectDescription to specifically handle operator family objects. It performs lookups in both pg_opfamily and pg_am catalogs to retrieve the operator family name and its associated access method name. The function formats a descriptive string in the form "operator family X for access method Y" where X is the (possibly schema-qualified) operator family name and Y is the access method name.

The function handles schema qualification by checking if the operator family is visible in the current search path using OpfamilyIsVisible(), and includes the schema name when necessary. It performs proper cache management by releasing both system cache entries after use.

## Parameters / Member Variables
- `buffer`: StringInfo buffer to append the description to
- `opfid`: OID of the operator family to describe
- `missing_ok`: If true, return silently for missing operator families instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookups for OPFAMILYOID and AMOID)
  - [OpfamilyIsVisible](../O/OpfamilyIsVisible.md) (visibility checking for operator families)
  - [get_namespace_name](get_namespace_name.md) (schema name retrieval)
  - quote_qualified_identifier (safe name quoting)
  - appendStringInfo (string formatting)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_opfamily and Form_pg_am structures

- Called from (representative examples):
  - [getObjectDescription](getObjectDescription.md) (main object description function, multiple locations for operator families and related objects)
  - object_type_map (object type mapping)

## Notes and Other Information
- Static function, only accessible within objectaddress.c
- Requires lookups in two different system catalogs (pg_opfamily and pg_am)
- Provides localized descriptions using gettext _() macro
- Always includes access method name in the description for context
- Automatically qualifies operator family names when not visible in search path
- Uses proper error handling with missing_ok parameter for operator family, but always errors for missing access method
- Appends to existing buffer rather than returning new string
- Critical for generating descriptions of access method operators and procedures
- Ensures proper memory management with system cache operations
- Access method lookup uses a stricter error policy (always fails if not found)