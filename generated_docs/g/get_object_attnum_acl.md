# get_object_attnum_acl

## Location
[src/backend/catalog/objectaddress.c:2684-2698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2684-L2698)

## Overview
Returns the attribute number for the access control list (ACL) field of a given PostgreSQL object class, enabling privilege management operations to locate the ACL data within catalog table structures.

## Definition
```c
AttrNumber get_object_attnum_acl(Oid class_id)
```

## Detailed Description
This function retrieves the ACL attribute number for a specific PostgreSQL object class. It provides a standardized way to access the column number that stores the access control list in the corresponding system catalog table. The ACL contains the privilege information for the object, including grants and permissions assigned to various roles. This function is essential for privilege management operations such as GRANT, REVOKE, and access control checking throughout the PostgreSQL system.

The function leverages the centralized object property system to maintain consistency across different object types and their privilege representations in the catalog tables.

## Parameters / Member Variables
- `class_id`: The OID of the object class (system catalog table OID like RelationRelationId, ProcedureRelationId, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [get_object_property_data](get_object_property_data.md)
  - ObjectPropertyType (struct)
  - ObjectType (enum)
- Called from (representative examples):
  - [ExecGrant_common](../E/ExecGrant_common.md)
  - [object_aclmask_ext](../o/object_aclmask_ext.md)
  - [recordExtObjInitPriv](../r/recordExtObjInitPriv.md)
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md)
  - ObjectAddressSet

## Notes and Other Information
- Returns the attribute number from the ObjectPropertyType structure's `attnum_acl` field
- Core component of PostgreSQL's privilege and access control system
- Used extensively in GRANT/REVOKE operations and privilege checking
- Essential for managing initial privileges and extension object privileges
- The returned attribute number corresponds to the column position in the system catalog table where the ACL array is stored
- Some object types may not have ACL columns, in which case the function behavior depends on the object property configuration