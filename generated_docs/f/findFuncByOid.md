# findFuncByOid

## Location
src/bin/pg_dump/common.c: 907 - 924

## Overview
Finds and returns the DumpableObject for a PostgreSQL function with the given OID, used in pg_dump operations for function lookup during database dumping.

## Definition
```c
FuncInfo *findFuncByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object management system. It searches for a function object by its OID (Object Identifier) and returns a pointer to the corresponding FuncInfo structure. The function creates a CatalogId using the ProcedureRelationId and the provided OID, then delegates to findObjectByCatalogId to locate the actual DumpableObject. It includes an assertion to ensure that any found object is indeed a function object (DO_FUNC).

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the function to find in the PostgreSQL system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - FuncInfo (struct)
  - DO_FUNC (enum value)
  - ProcedureRelationId (constant)
- Called from (representative examples):
  - [dumpProcLang](../d/dumpProcLang.md) (src/bin/pg_dump/pg_dump.c:12151, 12157, 12164)
  - [dumpCast](../d/dumpCast.md) (src/bin/pg_dump/pg_dump.c:12746)
  - [dumpTransform](../d/dumpTransform.md) (src/bin/pg_dump/pg_dump.c:12852, 12859)

## Notes and Other Information
- Returns NULL if no function with the specified OID is found
- Uses assertions to validate that found objects are actually function objects
- Part of the pg_dump object lookup infrastructure for database schema dumping
- Works specifically with function objects stored in the procedure relation
- Located in src/bin/pg_dump/common.c:907-924