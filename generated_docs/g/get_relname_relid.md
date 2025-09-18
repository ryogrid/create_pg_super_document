# get_relname_relid

## Location
src/backend/utils/cache/lsyscache.c: 1885 - 1898

## Overview
Given the name and namespace of a relation, looks up and returns the corresponding OID, or InvalidOid if no such relation exists.

## Definition
```c
Oid get_relname_relid(const char *relname, Oid relnamespace)
```

## Detailed Description
This function performs a system catalog lookup to find the OID of a relation (table, index, view, etc.) given its name and the OID of the namespace (schema) it belongs to. It uses the system cache for efficient retrieval from the pg_class catalog table. This is a fundamental function for name-to-OID resolution in PostgreSQL's catalog system, used extensively throughout the system when relation names need to be converted to their internal OID representation.

## Parameters / Member Variables
- `relname`: The name of the relation to look up
- `relnamespace`: The OID of the namespace (schema) containing the relation

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid2
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - RELNAMENSP (system cache identifier)
  - Anum_pg_class_oid
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - index_create
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [cluster](../c/cluster.md)
  - [CreateTableAsRelExists](../C/CreateTableAsRelExists.md)
  - [RenameRelationInternal](../R/RenameRelationInternal.md)
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)
  - [transformIndexConstraint](../t/transformIndexConstraint.md)

## Notes and Other Information
- Returns InvalidOid if the relation is not found, rather than raising an error
- This function is namespace-aware, meaning it only looks for relations within the specified schema
- Used extensively in DDL operations, parser functions, and catalog manipulation
- Part of the relation cache subsystem that provides efficient access to relation metadata
- The function uses the RELNAMENSP system cache for fast lookups by name and namespace