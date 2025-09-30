# get_object_catcache_name

## Location
[src/backend/catalog/objectaddress.c:2644-2651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2644-L2651)

## Overview
Retrieves the catalog cache identifier for name-based lookups of objects belonging to a given class.

## Definition
```c
int get_object_catcache_name(Oid class_id)
```

## Detailed Description
This function returns the catalog cache ID that corresponds to the name-based cache for a specific object class. While `get_object_catcache_oid` provides the cache for OID-based lookups, this function provides the cache ID for name-based lookups of catalog objects. PostgreSQL maintains separate caches for different types of lookups to optimize performance.

The function accesses the object property metadata for the given class and returns the `name_catcache_id` field, which identifies which catalog cache should be used for efficient object lookups by name (and potentially other identifying attributes like namespace).

## Parameters / Member Variables
- `class_id`: The OID of the catalog class (typically a system catalog table OID) for which to retrieve the name-based catalog cache ID

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md): Retrieves object property metadata
  - `ObjectPropertyType`: Structure containing object property information
- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md): Used during object renaming to check name conflicts
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md): Used during namespace changes to validate names
  - `ObjectAddressSet`: Used in object address construction for name-based lookups

## Notes and Other Information
- Returns an integer cache ID for name-based catalog cache lookups
- Complements `get_object_catcache_oid` by providing cache access for non-OID based lookups
- Essential for operations that need to resolve object names to OIDs or check for name conflicts
- The cache typically indexes objects by name and namespace (schema) for efficient resolution
- May return -1 or an invalid cache ID if no name-based cache is available for the object class
- Used primarily in DDL operations where objects are referenced by name rather than OID

## Simplified Source

```c
int get_object_catcache_name(Oid class_id) {
    // Get object property metadata for the catalog class
    const ObjectPropertyType *prop = get_object_property_data(class_id);

    // Return the name-based catalog cache ID
    return prop->name_catcache_id;
}
```