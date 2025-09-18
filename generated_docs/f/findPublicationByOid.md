# findPublicationByOid

## Location
src/bin/pg_dump/common.c: 1015 - 1032

## Overview
Finds and returns the DumpableObject for a PostgreSQL logical replication publication with the specified OID during the pg_dump process.

## Definition
```c
PublicationInfo *findPublicationByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object lookup system for PostgreSQL logical replication publications. It searches for a publication object by its Object Identifier (OID) and returns the corresponding PublicationInfo structure. Publications are used in PostgreSQL's logical replication feature to define a set of tables whose data changes are replicated. The function operates by creating a CatalogId structure with the publication's OID and utilizing the generic findObjectByCatalogId function to locate the object. It includes an assertion to verify that any found object is indeed of type DO_PUBLICATION, ensuring type safety during the dump process.

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the publication to find

## Dependencies
- Functions called/Symbols referenced:
  - findObjectByCatalogId
  - CatalogId (struct)
  - DumpableObject (struct)
  - PublicationInfo (struct)
  - DO_PUBLICATION (enum value)
  - PublicationRelationId (constant)
- Called from (representative examples):
  - getPublicationNamespaces (src/bin/pg_dump/pg_dump.c:4482)
  - getPublicationTables (src/bin/pg_dump/pg_dump.c:4588)

## Notes and Other Information
- Returns NULL if the publication with the given OID is not found
- Uses an assertion to ensure type safety - the found object must be of DO_PUBLICATION type
- Part of the pg_dump utility's support for logical replication features
- Publications are a key component of PostgreSQL's logical replication system
- The function follows the same pattern as other findXXXByOid functions in the codebase
- Used when dumping publication-related metadata and relationships