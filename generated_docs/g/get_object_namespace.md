# get_object_namespace

## Location
[src/backend/catalog/objectaddress.c:2564-2599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2564-L2599)

## Overview
Retrieves the schema (namespace) OID that contains a specified database object, returning InvalidOid for objects that do not belong to any namespace.

## Definition


## Detailed Description
The `get_object_namespace` function determines which schema contains a given database object by looking up the object's namespace attribute in the system catalogs. This function is essential for namespace-aware operations and resolving object names within their proper schema context.

The function first checks if the object type supports namespace ownership by examining the object property data. If the object type has no namespace attribute (attnum_namespace is InvalidAttrNumber), it immediately returns InvalidOid, indicating the object is not schema-scoped.

For objects that do have namespace ownership, the function uses the appropriate system cache to retrieve the object's catalog tuple and extracts the namespace OID from the namespace attribute. The function relies on PostgreSQL's system cache infrastructure for efficient access to catalog data and includes proper error handling for cache lookup failures.

## Parameters / Member Variables
- `address`: Pointer to ObjectAddress structure containing the object identification (classId, objectId, objectSubId)

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [RemoveObjects](../R/RemoveObjects.md) (src/backend/commands/dropcmds.c:102)
  - ObjectAddressSet (src/include/catalog/objectaddress.h:55)

## Notes and Other Information
- Returns InvalidOid for objects that are not owned by any namespace (e.g., databases, tablespaces, roles)
- Requires the object type to have a system cache configured; currently cannot handle object types without caches
- Uses efficient system cache lookups rather than direct catalog table access
- The function is used in dependency tracking and object resolution scenarios
- Essential for implementing schema-qualified object names and namespace-aware operations
- Provides proper error reporting for objects that should exist but are not found in the cache