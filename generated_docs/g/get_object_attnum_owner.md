# get_object_attnum_owner

## Location
[src/backend/catalog/objectaddress.c:2676-2683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2676-L2683)

## Overview
Returns the attribute number for the owner field of a given PostgreSQL object class, enabling access control and ownership operations to locate the owner identifier within catalog table structures.

## Definition
```c
AttrNumber get_object_attnum_owner(Oid class_id)
```

## Detailed Description
This function provides access to the owner attribute number for a specific PostgreSQL object class. It serves as a centralized mechanism to retrieve the column number that stores the owner OID in the corresponding system catalog table. This information is fundamental for access control checks, ownership verification, privilege management, and owner modification operations throughout the PostgreSQL system.

The function integrates with the object property data system to maintain consistency and provide a uniform interface for accessing owner information across different types of database objects.

## Parameters / Member Variables
- `class_id`: The OID of the object class (system catalog table OID such as RelationRelationId, ProcedureRelationId, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md)
  - ObjectPropertyType (struct)
- Called from (representative examples):
  - [ExecGrant_common](../E/ExecGrant_common.md)
  - [object_aclmask_ext](../o/object_aclmask_ext.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [RemoveRoleFromInitPriv](../R/RemoveRoleFromInitPriv.md)
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md)
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md)
  - ObjectAddressSet

## Notes and Other Information
- Returns the attribute number from the ObjectPropertyType structure's `attnum_owner` field
- Critical component of PostgreSQL's access control system
- Used extensively in privilege checking and ownership validation operations
- Essential for ALTER OWNER commands and access control list (ACL) operations
- The returned attribute number corresponds to the column position in the system catalog table where the owner OID is stored

## Simplified Source

```c
AttrNumber get_object_attnum_owner(Oid class_id)
{
    const ObjectPropertyType *prop = get_object_property_data(class_id);
    return prop->attnum_owner;
}
```