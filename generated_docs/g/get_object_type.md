# get_object_type

## Location
src/bin/pg_dump/filter.c: 123 - 154

## Overview
Returns the object type associated with a given database object, with special handling for table-like objects to provide more precise type information.

## Definition
```c
ObjectType get_object_type(Oid class_id, Oid object_id)
```

## Detailed Description
This function determines the ObjectType for a database object identified by its class ID and object ID. It is primarily used for generating precise error messages in ACL (Access Control List) checks. The function first retrieves the basic object property data, but provides special handling for table-like objects by examining the relation kind to return more specific object types (e.g., distinguishing between tables, views, indexes, etc.).

## Parameters / Member Variables
- `class_id`: OID of the system catalog that contains the object
- `object_id`: OID of the specific object within that catalog

## Dependencies
- Functions called/Symbols referenced:
  - get_object_property_data
  - get_relkind_objtype
  - get_rel_relkind
  - OBJECT_TABLE (constant)
- Called from (representative examples):
  - ExecGrant_common (in aclchk.c)
  - object_aclmask_ext (in aclchk.c)
  - AlterObjectRename_internal (in alter.c)
  - AlterObjectNamespace_internal (in alter.c)
  - AlterObjectOwner_internal (in alter.c)
  - filter_read_item (in filter.c)

## Notes and Other Information
- Designed to avoid failing to ensure reliable error message generation
- Provides enhanced type resolution for table-like objects by examining relation kinds
- Part of the object address infrastructure used throughout PostgreSQL's catalog system
- The function helps distinguish between different types of relations (tables, views, indexes, etc.) for more precise error reporting
- Used extensively in privilege checking and object management operations