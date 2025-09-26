# get_catalog_object_by_oid

## Location
[src/backend/catalog/objectaddress.c:2781-2793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2781-L2793)

## Overview
Retrieves a copy of a catalog tuple for a given object OID from a specified catalog relation, using system cache optimization when available.

## Definition

```c
HeapTuple
get_catalog_object_by_oid(Relation catalog, AttrNumber oidcol, Oid objectId)
```
## Detailed Description
This function serves as a convenience wrapper around get_catalog_object_by_oid_extended, providing the most common use case for catalog object retrieval. It attempts to locate a database object by its OID within a specified catalog table, returning a copy of the corresponding HeapTuple if found.

The function leverages PostgreSQL's system cache (syscache) when available for improved performance. If the requested object OID is not found in the catalog, the function returns NULL rather than throwing an error, allowing callers to handle missing objects gracefully.

## Parameters / Member Variables
- `catalog`: Open Relation representing the catalog table to search (must be opened and locked by caller)
- `oidcol`: Column number (AttrNumber) containing the object OID within the catalog table
- `objectId`: The OID of the object to retrieve from the catalog

## Dependencies
- Functions called/Symbols referenced:
  - [get_catalog_object_by_oid_extended](get_catalog_object_by_oid_extended.md) (with missing_ok=false)
- Called from (representative examples):
  - [pg_identify_object](../p/pg_identify_object.md)
  - [getConstraintTypeDescription](getConstraintTypeDescription.md)
  - [getObjectIdentityParts](getObjectIdentityParts.md) (multiple calls)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md)
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md)
  - ObjectAddressSet

## Notes and Other Information
- Simple wrapper function that calls get_catalog_object_by_oid_extended with missing_ok=false
- Returns NULL if the object OID is not found in the catalog
- Caller is responsible for opening and properly locking the catalog relation
- Utilizes system cache (syscache) for performance optimization when available
- Located in src/backend/catalog/objectaddress.c:2781-2793
- Returns a copy of the tuple, so caller is responsible for freeing the memory