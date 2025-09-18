# RelationGetPrimaryKeyIndex

## Location
src/backend/utils/cache/relcache.c: 4997 - 5017

## Overview
RelationGetPrimaryKeyIndex retrieves the OID of a relation's primary key index, returning InvalidOid if no such index exists or if the primary key is deferrable.

## Definition
```c
Oid RelationGetPrimaryKeyIndex(Relation relation)
```

## Detailed Description
This function provides access to the primary key index OID for a given relation. It ensures that the relation's index information is up-to-date by calling RelationGetIndexList if necessary. The function implements a key design decision in PostgreSQL: deferrable primary keys are not considered "true" primary key indexes for certain operations, so the function returns InvalidOid for such constraints.

The function operates by:
1. Checking if the relation's index information is valid (rd_indexvalid)
2. If not valid, refreshing the index list through RelationGetIndexList
3. Returning the cached primary key index OID, but only if it's not deferrable

## Parameters / Member Variables
- `relation`: The relation (table) for which to retrieve the primary key index OID

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetIndexList
  - list_free
- Called from (representative examples):
  - GetRelationIdentityOrPK
  - RelationPtr

## Notes and Other Information
- Returns InvalidOid if there is no primary key index
- Returns InvalidOid if the primary key constraint is DEFERRABLE 
- The function relies on cached information in the relation descriptor (rd_pkindex, rd_ispkdeferrable)
- The distinction between deferrable and non-deferrable primary keys is important for replication and logical decoding operations
- Located in src/backend/utils/cache/relcache.c:4997-5017