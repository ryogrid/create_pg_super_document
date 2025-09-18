# IsCatalogRelationOid

## Location
src/backend/catalog/catalog.c: 120 - 151

## Overview
IsCatalogRelationOid determines whether a relation OID corresponds to a system catalog by checking if the OID is in the "pinned" range created during bootstrap.

## Definition
```c
bool IsCatalogRelationOid(Oid relid)
```

## Detailed Description
This function implements the core logic for identifying system catalog relations in PostgreSQL by examining their OIDs. It uses a simple but reliable test: relations with OIDs less than FirstUnpinnedObjectId are considered system catalogs. This includes all catalogs defined during bootstrap, their indexes, and their TOAST tables and indexes.

The approach relies on PostgreSQL's OID allocation strategy where system objects created during initdb are assigned "pinned" OIDs in a reserved range. This test explicitly excludes information_schema relations, which are not considered integral to the system and can be dropped and recreated without affecting core functionality.

The function is designed to be extremely lightweight, performing no catalog accesses and relying only on OID comparison, making it safe for use in any context including bootstrap and recovery scenarios.

## Parameters / Member Variables
- `relid`: The OID of the relation to be checked

## Dependencies
- Functions called/Symbols referenced:
  - FirstUnpinnedObjectId (constant defining the boundary between pinned and unpinned OIDs)
- Called from (representative examples):
  - IsSystemClass
  - IsCatalogRelation
  - is_publishable_class
  - ReindexMultipleTables
  - read_stream_begin_relation

## Notes and Other Information
- Uses OID comparison as the sole criterion for identification
- Relies on FirstUnpinnedObjectId boundary to distinguish system catalogs
- Excludes information_schema relations which are not considered integral to the system
- The test is reliable because OID wraparound skips the pinned OID range
- No catalog accesses are performed, making it safe for bootstrap and recovery scenarios
- More efficient than catalog-based identification methods
- The function is located in src/backend/catalog/catalog.c:120-151