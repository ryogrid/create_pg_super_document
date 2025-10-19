# findOprByOid

## Location
[src/bin/pg_dump/common.c:925-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L925-L942)

## Overview
Finds and returns the DumpableObject for a PostgreSQL operator with the given OID, used in pg_dump operations for operator lookup during database dumping.

## Definition
```c
OprInfo *findOprByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object management system. It searches for an operator object by its OID (Object Identifier) and returns a pointer to the corresponding OprInfo structure. The function creates a CatalogId using the OperatorRelationId and the provided OID, then delegates to findObjectByCatalogId to locate the actual DumpableObject. It includes an assertion to ensure that any found object is indeed an operator object (DO_OPERATOR).

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the operator to find in the PostgreSQL system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - [OprInfo](../O/OprInfo.md) (struct)
  - DO_OPERATOR (enum value)
  - OperatorRelationId (constant)
- Called from (representative examples):
  - [getFormattedOperatorName](../g/getFormattedOperatorName.md) (src/bin/pg_dump/pg_dump.c:13230)

## Notes and Other Information
- Returns NULL if no operator with the specified OID is found
- Uses assertions to validate that found objects are actually operator objects
- Part of the pg_dump object lookup infrastructure for database schema dumping
- Works specifically with operator objects stored in the operator relation
- Located in src/bin/pg_dump/common.c:925-942
- Has fewer call sites compared to findTypeByOid and findFuncByOid, primarily used for operator name formatting

## Simplified Source

```c
OprInfo *findOprByOid(Oid oid) {
    // Create catalog ID for operator lookup
    CatalogId catId;
    catId.tableoid = OperatorRelationId;
    catId.oid = oid;

    // Find object and return as OprInfo
    DumpableObject *dobj = findObjectByCatalogId(catId);
    return (OprInfo *) dobj;  // Returns NULL if not found
}
```