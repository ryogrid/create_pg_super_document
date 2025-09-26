# GetRelationPublications

## Location
[src/backend/catalog/pg_publication.c:687-715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L687-L715)

## Overview
Retrieves a list of publication OIDs associated with a specific relation, used to determine which publications include a given table.

## Definition
```c
List *GetRelationPublications(Oid relid)
```

## Detailed Description
This function searches the PostgreSQL system catalog to find all publications that include a specific relation (table). It uses the system cache to efficiently look up entries in the pg_publication_rel catalog table, which maintains the mapping between publications and their associated relations. The function returns a list of publication OIDs that can be used by other parts of the system to determine publication membership for replication and other purposes.

The function uses the PUBLICATIONRELMAP system cache to perform the lookup, which provides efficient access to the pg_publication_rel catalog. It iterates through all matching catalog entries and extracts the publication OID from each tuple.

## Parameters / Member Variables
- `relid`: The OID of the relation (table) for which to find associated publications

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList1
  - [lappend_oid](../l/lappend_oid.md)
  - ReleaseSysCacheList
  - Form_pg_publication_rel
  - [CatCList](../C/CatCList.md)
- Called from (representative examples):
  - [GetTopMostAncestorInPublication](GetTopMostAncestorInPublication.md)
  - [ATPrepChangePersistence](../A/ATPrepChangePersistence.md)
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md)

## Notes and Other Information
- Uses system cache for efficient catalog lookup via PUBLICATIONRELMAP
- Returns NIL if no publications are found for the relation
- Properly manages memory by releasing the system cache list after use
- The returned list contains publication OIDs that can be used for further publication-related operations
- This function is commonly used in replication contexts to determine which publications need to be considered for a given table