# get_object_address_publication_rel

## Location
src/backend/catalog/objectaddress.c: 1863 - 1915

## Overview
Finds and returns the ObjectAddress for a publication relation mapping by resolving both the relation name and publication name to locate the corresponding pg_publication_rel catalog entry.

## Definition
```c
static ObjectAddress get_object_address_publication_rel(List *object, Relation *relp, bool missing_ok)
```

## Detailed Description
This function resolves a publication relation object address by taking a list containing a relation name and publication name, then performing lookups to find the corresponding publication-relation mapping. It first opens the specified relation using relation_openrv_extended, then looks up the publication by name, and finally searches for the mapping entry in pg_publication_rel that connects the specific relation to the publication.

The function returns both the ObjectAddress of the publication relation mapping and sets the relp output parameter to point to the opened relation, allowing the caller to access the relation if needed. The relation remains open and must be closed by the caller.

## Parameters / Member Variables
- `object`: List containing exactly two elements - the relation name (as a list of name components) and the publication name (as a string)
- `relp`: Output parameter that receives a pointer to the opened Relation structure
- `missing_ok`: Boolean flag indicating whether to return an invalid ObjectAddress (true) or raise an error (false) when the publication relation mapping is not found

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddressSet
  - linitial/lsecond (list manipulation)
  - relation_openrv_extended
  - makeRangeVarFromNameList
  - GetPublicationByName
  - relation_close
  - GetSysCacheOid2 (PUBLICATIONRELMAP cache lookup)
  - RelationGetRelid/RelationGetRelationName
  - ObjectIdGetDatum
- Called from (representative examples):
  - get_object_address (main object address resolution dispatcher)
  - object_type_map (object type mapping table)

## Notes and Other Information
- Uses PUBLICATIONRELMAP system cache index for efficient publication-relation mapping lookup
- Returns an ObjectAddress with PublicationRelRelationId as the class ID and the publication relation mapping OID as the object ID
- The relation is opened with AccessShareLock and remains open for the caller to use
- Error messages include both relation name and publication name for better diagnostics
- Part of PostgreSQL's logical replication system for tracking which relations are included in publications
- The caller is responsible for closing the relation returned in the relp parameter