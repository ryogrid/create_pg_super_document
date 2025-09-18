# get_object_attnum_owner

## Location
src/backend/catalog/objectaddress.c: 2676 - 2683

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
  - get_object_property_data
  - ObjectPropertyType (struct)
- Called from (representative examples):
  - ExecGrant_common
  - object_aclmask_ext
  - object_ownercheck
  - RemoveRoleFromInitPriv
  - AlterObjectRename_internal
  - AlterObjectNamespace_internal
  - AlterObjectOwner_internal
  - ObjectAddressSet

## Notes and Other Information
- Returns the attribute number from the ObjectPropertyType structure's `attnum_owner` field
- Critical component of PostgreSQL's access control system
- Used extensively in privilege checking and ownership validation operations
- Essential for ALTER OWNER commands and access control list (ACL) operations
- The returned attribute number corresponds to the column position in the system catalog table where the owner OID is stored