# findTypeByOid

## Location
src/bin/pg_dump/common.c: 888 - 906

## Overview
Finds and returns the DumpableObject for a PostgreSQL type with the given OID, used in pg_dump operations for type lookup during database dumping.

## Definition


## Detailed Description
This function is part of the pg_dump utility's object management system. It searches for a type object by its OID (Object Identifier) and returns a pointer to the corresponding TypeInfo structure. The function creates a CatalogId using the TypeRelationId and the provided OID, then delegates to findObjectByCatalogId to locate the actual DumpableObject. It includes assertions to ensure that any found object is indeed a type object (either DO_TYPE or DO_DUMMY_TYPE).

## Parameters / Member Variables
- : The Object Identifier (OID) of the type to find in the PostgreSQL system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct) 
  - [TypeInfo](../T/TypeInfo.md) (struct)
  - DO_TYPE (enum value)
  - DO_DUMMY_TYPE (enum value)
- Called from (representative examples):
  - [getCasts](../g/getCasts.md) (src/bin/pg_dump/pg_dump.c:8672-8673)
  - [getTransforms](../g/getTransforms.md) (src/bin/pg_dump/pg_dump.c:8778)
  - [collectComments](../c/collectComments.md) (src/bin/pg_dump/pg_dump.c:10497)
  - [collectSecLabels](../c/collectSecLabels.md) (src/bin/pg_dump/pg_dump.c:15693)
  - [getFormattedTypeName](../g/getFormattedTypeName.md) (src/bin/pg_dump/pg_dump.c:18958)

## Notes and Other Information
- Returns NULL if no type with the specified OID is found
- Uses assertions to validate that found objects are actually type objects
- Part of the pg_dump object lookup infrastructure for database schema dumping
- Works with both regular types (DO_TYPE) and dummy types (DO_DUMMY_TYPE)
- Located in src/bin/pg_dump/common.c:888-906