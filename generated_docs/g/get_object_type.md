# get_object_type

## Location
[src/bin/pg_dump/filter.c:123-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/filter.c#L123-L154)

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
  - [get_object_property_data](get_object_property_data.md)
  - [get_relkind_objtype](get_relkind_objtype.md)
  - [get_rel_relkind](get_rel_relkind.md)
  - OBJECT_TABLE (constant)
- Called from (representative examples):
  - [ExecGrant_common](../E/ExecGrant_common.md) (in aclchk.c)
  - [object_aclmask_ext](../o/object_aclmask_ext.md) (in aclchk.c)
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (in alter.c)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md) (in alter.c)
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md) (in alter.c)
  - [filter_read_item](../f/filter_read_item.md) (in filter.c)

## Notes and Other Information
- Designed to avoid failing to ensure reliable error message generation
- Provides enhanced type resolution for table-like objects by examining relation kinds
- Part of the object address infrastructure used throughout PostgreSQL's catalog system
- The function helps distinguish between different types of relations (tables, views, indexes, etc.) for more precise error reporting
- Used extensively in privilege checking and object management operations

## Simplified Source

```c
ObjectType get_object_type(Oid class_id, Oid object_id)
{
    const ObjectPropertyType *prop = get_object_property_data(class_id);

    // Special handling for table-like objects to get precise type
    if (prop->objtype == OBJECT_TABLE) {
        // Get specific relation kind (table, view, index, etc.)
        return get_relkind_objtype(get_rel_relkind(object_id));
    }
    else {
        return prop->objtype;
    }
}
```