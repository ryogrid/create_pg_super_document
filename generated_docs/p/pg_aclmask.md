# pg_aclmask

## Location
[src/backend/catalog/aclchk.c:3036-3100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3036-L3100)

## Overview
Routes ACL (Access Control List) mask operations to appropriate object-specific functions based on the object type, serving as a central dispatcher for permission checking across different PostgreSQL object types.

## Definition

```c
static AclMode
pg_aclmask(ObjectType objtype, Oid object_oid, AttrNumber attnum, Oid roleid,
		   AclMode mask, AclMaskHow how)
```
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
  - [pg_class_aclmask](pg_class_aclmask.md)
  - [pg_attribute_aclmask](pg_attribute_aclmask.md)
  - [object_aclmask](../o/object_aclmask.md)
  - [pg_largeobject_aclmask_snapshot](pg_largeobject_aclmask_snapshot.md)
  - [pg_parameter_acl_aclmask](pg_parameter_acl_aclmask.md)
  - ObjectType enum constants (OBJECT_COLUMN, OBJECT_TABLE, etc.)
  - ACL_NO_RIGHTS constant
- Called from (representative examples):
  - InternalDefaultACL
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md)

## Notes and Other Information
- This is a static function internal to the aclchk.c module, not exposed in the public API
- The function uses a switch statement to efficiently route requests based on object type
- Error handling is explicit for unsupported object types (statistics objects and event triggers)
- Column access combines both table and attribute permissions using bitwise OR
- Most object types delegate to the generic `object_aclmask` function with their respective system catalog relation IDs

## Simplified Source

```c
static AclMode pg_aclmask(ObjectType objtype, Oid object_oid, AttrNumber attnum,
                         Oid roleid, AclMode mask, AclMaskHow how) {
    switch (objtype) {
        case OBJECT_COLUMN:
            // Combine table and column permissions
            return pg_class_aclmask(object_oid, roleid, mask, how) |
                   pg_attribute_aclmask(object_oid, attnum, roleid, mask, how);

        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
            return pg_class_aclmask(object_oid, roleid, mask, how);

        case OBJECT_DATABASE:
            return object_aclmask(DatabaseRelationId, object_oid, roleid, mask, how);

        case OBJECT_FUNCTION:
            return object_aclmask(ProcedureRelationId, object_oid, roleid, mask, how);

        case OBJECT_LANGUAGE:
            return object_aclmask(LanguageRelationId, object_oid, roleid, mask, how);

        case OBJECT_LARGEOBJECT:
            return pg_largeobject_aclmask_snapshot(object_oid, roleid, mask, how, NULL);

        case OBJECT_PARAMETER_ACL:
            return pg_parameter_acl_aclmask(object_oid, roleid, mask, how);

        case OBJECT_SCHEMA:
            return object_aclmask(NamespaceRelationId, object_oid, roleid, mask, how);

        case OBJECT_TABLESPACE:
            return object_aclmask(TableSpaceRelationId, object_oid, roleid, mask, how);

        case OBJECT_FDW:
            return object_aclmask(ForeignDataWrapperRelationId, object_oid, roleid, mask, how);

        case OBJECT_FOREIGN_SERVER:
            return object_aclmask(ForeignServerRelationId, object_oid, roleid, mask, how);

        case OBJECT_TYPE:
            return object_aclmask(TypeRelationId, object_oid, roleid, mask, how);

        case OBJECT_STATISTIC_EXT:
        case OBJECT_EVENT_TRIGGER:
            // These object types don't support grantable rights
            elog(ERROR, "grantable rights not supported for this object type");
            return ACL_NO_RIGHTS;

        default:
            elog(ERROR, "unrecognized object type: %d", (int) objtype);
            return ACL_NO_RIGHTS;
    }
}
```