# get_object_property_data

## Location
src/backend/catalog/objectaddress.c: 2746 - 2780

## Overview
Retrieves the ObjectProperty structure for a given object class identifier, providing metadata about how to handle objects of that class.

## Definition


## Detailed Description
This internal function locates and returns the ObjectPropertyType structure corresponding to a specific object class ID. It serves as the primary lookup mechanism for object metadata in PostgreSQL's object address subsystem. The function includes an optimization using a static cache (prop_last) to speed up consecutive lookups of the same object class, which is a common usage pattern.

If the requested class_id is not found in the ObjectProperty table, the function reports an ERROR with an internal error message, indicating an unrecognized class ID. This makes it a critical validation point for object class support.

## Parameters / Member Variables
- `class_id`: Object identifier (Oid) representing the class of database object to look up

## Dependencies
- Functions called/Symbols referenced:
  - ObjectPropertyType (return type and array element type)
  - lengthof (macro for array length)
  - ereport, ERROR, errmsg_internal (error reporting)
- Called from (representative examples):
  - object_type_map
  - [get_object_namespace](get_object_namespace.md)
  - [get_object_class_descr](get_object_class_descr.md)
  - [get_object_oid_index](get_object_oid_index.md)
  - [get_object_catcache_oid](get_object_catcache_oid.md)
  - [get_object_catcache_name](get_object_catcache_name.md)
  - [get_object_attnum_oid](get_object_attnum_oid.md)
  - [get_object_attnum_name](get_object_attnum_name.md)
  - [get_object_attnum_namespace](get_object_attnum_namespace.md)
  - [get_object_attnum_owner](get_object_attnum_owner.md)
  - [get_object_attnum_acl](get_object_attnum_acl.md)
  - [get_object_type](get_object_type.md)
  - [get_object_namensp_unique](get_object_namensp_unique.md)

## Notes and Other Information
- Static function (internal to objectaddress.c) for object property lookup
- Includes caching optimization via static prop_last variable for performance
- Throws ERROR for unrecognized class IDs rather than returning NULL
- Located in src/backend/catalog/objectaddress.c:2746-2780
- Central to the object address subsystem's metadata retrieval functionality
- The NULL return at the end is only to satisfy compiler warnings and should never be reached