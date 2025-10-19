# findCollationByOid

## Location
[src/bin/pg_dump/common.c:961-978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L961-L978)

## Overview
Finds and returns the DumpableObject for a PostgreSQL collation with the given OID, used in pg_dump operations for collation lookup during database dumping.

## Definition
```c
CollInfo *findCollationByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object management system. It searches for a collation object by its OID (Object Identifier) and returns a pointer to the corresponding CollInfo structure. The function creates a CatalogId using the CollationRelationId and the provided OID, then delegates to findObjectByCatalogId to locate the actual DumpableObject. It includes an assertion to ensure that any found object is indeed a collation object (DO_COLLATION).

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the collation to find in the PostgreSQL system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - [CollInfo](../C/CollInfo.md) (struct)
  - DO_COLLATION (enum value)
  - CollationRelationId (constant)
- Called from (representative examples):
  - [dumpRangeType](../d/dumpRangeType.md) (src/bin/pg_dump/pg_dump.c:11182)
  - [dumpDomain](../d/dumpDomain.md) (src/bin/pg_dump/pg_dump.c:11637)
  - [dumpCompositeType](../d/dumpCompositeType.md) (src/bin/pg_dump/pg_dump.c:11898)
  - [createDummyViewAsClause](../c/createDummyViewAsClause.md) (src/bin/pg_dump/pg_dump.c:15929)
  - [dumpTableSchema](../d/dumpTableSchema.md) (src/bin/pg_dump/pg_dump.c:16206)

## Notes and Other Information
- Returns NULL if no collation with the specified OID is found
- Uses assertions to validate that found objects are actually collation objects
- Part of the pg_dump object lookup infrastructure for database schema dumping
- Works specifically with collation objects that define sorting and character classification rules
- Located in src/bin/pg_dump/common.c:961-978
- Widely used in type dumping operations where collation information is needed

## Simplified Source

```c
CollInfo *findCollationByOid(Oid oid) {
    // Create catalog ID for collation lookup
    CatalogId catId;
    catId.tableoid = CollationRelationId;
    catId.oid = oid;

    // Find object and return as CollInfo
    DumpableObject *dobj = findObjectByCatalogId(catId);
    return (CollInfo *) dobj;  // Returns NULL if not found
}
```