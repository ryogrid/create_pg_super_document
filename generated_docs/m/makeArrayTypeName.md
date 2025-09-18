# makeArrayTypeName

## Location
src/backend/catalog/pg_type.c: 840 - 904

## Overview
makeArrayTypeName generates a unique array type name for a given base type by following PostgreSQL's naming convention of prepending an underscore while handling name length limits and conflicts.

## Definition


## Detailed Description
makeArrayTypeName implements PostgreSQL's traditional naming convention for array types, which prepends an underscore to the base type name (e.g., "int4" becomes "_int4"). The function handles the complexities that arise when this simple rule encounters practical constraints.

The function addresses two main challenges: name length limitations and naming conflicts. When the resulting array name would exceed PostgreSQL's maximum identifier length (NAMEDATALEN), it truncates the base type name appropriately. When the desired array name conflicts with an existing type, it appends additional underscores and numeric suffixes to ensure uniqueness.

The name generation process leverages makeObjectName() with an empty first component, which handles the underscore prepending and length truncation automatically. The function then iteratively tests for conflicts using the system cache, incrementing a numeric suffix until a unique name is found. This approach is similar to the strategy used by ChooseRelationName() for generating unique relation names.

## Parameters
- : The name of the base type for which to generate an array type name
- : The OID of the namespace where the array type will be created

## Dependencies
- Functions called/Symbols referenced:
  - makeObjectName
  - SearchSysCacheExists2
  - CStringGetDatum, ObjectIdGetDatum
  - pfree
  - snprintf
- Called from (representative examples):
  - heap_create_with_catalog (src/backend/catalog/heap.c:1357)
  - RenameTypeInternal (src/backend/catalog/pg_type.c:825)
  - moveArrayTypeName (src/backend/catalog/pg_type.c:926)
  - DefineType (src/backend/commands/typecmds.c:610)
  - DefineDomain (src/backend/commands/typecmds.c:1060)
  - DefineEnum (src/backend/commands/typecmds.c:1226)
  - DefineRange (src/backend/commands/typecmds.c:1637, 1676)

## Notes and Other Information
- Follows PostgreSQL's ancient tradition of prefixing array type names with underscore
- The caller is responsible for calling pfree() on the returned string
- Uses incremental numeric suffixes (starting from 1) to resolve naming conflicts
- Truncates base type names from the right when necessary to fit within NAMEDATALEN
- The naming strategy prioritizes compatibility with existing client code that expects the underscore convention
- Critical for maintaining consistent array type naming across PostgreSQL's type system
- Returns a dynamically allocated string that must be freed by the caller