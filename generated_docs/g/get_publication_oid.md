# get_publication_oid

## Location
src/backend/utils/cache/lsyscache.c: 3625 - 3644

## Overview
Looks up the object identifier (OID) of a publication given its name, with optional error handling for missing publications.

## Definition
```c
Oid get_publication_oid(const char *pubname, bool missing_ok)
```

## Detailed Description
This function performs a lookup in the PostgreSQL system cache to find the OID of a publication identified by its name. Publications are used in PostgreSQL's logical replication system to define which tables and operations should be replicated to subscribers.

The function uses the PUBLICATIONNAME system cache to efficiently retrieve the publication's OID. The behavior when a publication is not found depends on the missing_ok parameter: if false, an error is raised; if true, InvalidOid is returned instead.

## Parameters / Member Variables
- `pubname`: The name of the publication to look up
- `missing_ok`: If false, throw an error when publication is not found; if true, return InvalidOid instead

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid1
  - CStringGetDatum
  - OidIsValid
  - ereport
  - errcode
  - errmsg
- Called from (representative examples):
  - get_object_address_unqualified
  - GetPublicationByName

## Notes and Other Information
- This function is part of the logical replication infrastructure in PostgreSQL
- Publications are objects that define a set of tables whose data changes are published
- The function provides both strict (error-throwing) and lenient (InvalidOid-returning) lookup modes
- Located in src/backend/utils/cache/lsyscache.c:3625-3644
- Uses the PUBLICATIONNAME system cache for efficient lookups