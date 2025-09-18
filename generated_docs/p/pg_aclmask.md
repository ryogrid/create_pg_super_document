# pg_aclmask

## Location
src/backend/catalog/aclchk.c: 3036 - 3100

## Overview
Routes ACL (Access Control List) mask operations to appropriate object-specific functions based on the object type, serving as a central dispatcher for permission checking across different PostgreSQL object types.

## Definition


## Detailed Description
The `pg_aclmask` function serves as a central relay mechanism for PostgreSQL's access control system. It dispatches permission checking requests to the appropriate specialized ACL functions based on the object type being accessed. This design provides a unified interface for checking permissions across different types of database objects while delegating the actual permission logic to type-specific implementations.

The function handles various PostgreSQL object types including tables, sequences, databases, functions, languages, large objects, parameter ACLs, schemas, tablespaces, foreign data wrappers, foreign servers, and custom types. For unsupported object types like statistics objects and event triggers, it explicitly returns errors indicating that grantable rights are not supported.

For column-level access, the function combines both table-level and attribute-level permissions by performing a bitwise OR operation between the results of `pg_class_aclmask` and `pg_attribute_aclmask`.

## Parameters / Member Variables
- `objtype`: The type of database object being checked (ObjectType enum value)
- `object_oid`: The OID of the specific object being accessed
- `attnum`: The attribute number for column-level access (relevant only for OBJECT_COLUMN)
- `roleid`: The OID of the role whose permissions are being checked
- `mask`: The access permissions being requested (AclMode bitmask)
- `how`: Specifies how the ACL checking should be performed (AclMaskHow enum)

## Dependencies
- Functions called/Symbols referenced:
  - pg_class_aclmask
  - pg_attribute_aclmask
  - object_aclmask
  - pg_largeobject_aclmask_snapshot
  - pg_parameter_acl_aclmask
  - ObjectType enum constants (OBJECT_COLUMN, OBJECT_TABLE, etc.)
  - ACL_NO_RIGHTS constant
- Called from (representative examples):
  - InternalDefaultACL
  - restrict_and_check_grant

## Notes and Other Information
- This is a static function internal to the aclchk.c module, not exposed in the public API
- The function uses a switch statement to efficiently route requests based on object type
- Error handling is explicit for unsupported object types (statistics objects and event triggers)
- Column access combines both table and attribute permissions using bitwise OR
- Most object types delegate to the generic `object_aclmask` function with their respective system catalog relation IDs