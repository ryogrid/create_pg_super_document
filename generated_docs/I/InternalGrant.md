# InternalGrant

## Location
[src/include/utils/aclchk_internal.h:42-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/aclchk_internal.h#L42-L45)

## Overview
InternalGrant is a structure that represents Grant/Revoke statements in PostgreSQL internal format, where object and grantee names have been transformed from their string representations into Oids and privileges are represented as AclMode bitmasks.

## Definition
```c
typedef struct
{
    bool        is_grant;
    ObjectType  objtype;
    List       *objects;
    bool        all_privs;
    AclMode     privileges;
    List       *col_privs;
    List       *grantees;
    bool        grant_option;
    DropBehavior behavior;
} InternalGrant;
```

## Detailed Description
The InternalGrant structure serves as an internal representation of SQL GRANT and REVOKE statements after they have been parsed and transformed. This structure bridges the gap between the external SQL syntax and the internal privilege management system in PostgreSQL.

The structure handles both object-level and column-level privileges. Object-level privileges are represented by the `all_privs` and `privileges` fields, while column-level privileges (valid only for tables) are stored separately in the `col_privs` list as untransformed AccessPriv nodes.

A key feature is that if `privileges` is set to ACL_NO_RIGHTS (0) and `all_privs` is true, the privileges field will be internally modified to the appropriate ACL_ALL_RIGHTS_* constant based on the object type, effectively modifying the InternalGrant struct during processing.

## Parameters / Member Variables
- `is_grant`: Boolean flag indicating whether this is a GRANT (true) or REVOKE (false) operation
- `objtype`: The type of database object being granted/revoked privileges on (table, function, database, etc.)
- `objects`: List of Oids representing the specific database objects targeted by the grant/revoke
- `all_privs`: Boolean indicating whether all available privileges for the object type should be granted/revoked
- `privileges`: AclMode bitmask representing the specific privileges being granted/revoked at object level
- `col_privs`: List of untransformed AccessPriv nodes representing column-level privilege specifications (only valid for OBJECT_TABLE)
- `grantees`: List of Oids representing the roles/users receiving or losing the privileges
- `grant_option`: Boolean indicating whether the WITH GRANT OPTION clause was specified
- `behavior`: DropBehavior specifying how to handle dependent objects (CASCADE or RESTRICT for revoke operations)

## Dependencies
- Types used:
  - ObjectType (from nodes/parsenodes.h)
  - [List](../L/List.md) (from nodes/pg_list.h)
  - AclMode (access control mode bitmask)
  - DropBehavior (dependency handling behavior)
- Used extensively by:
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md) (main entry point for processing grant statements)
  - [ExecGrantStmt_oids](../E/ExecGrantStmt_oids.md) (dispatcher for different object types)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md) (handles table/sequence grants)
  - [ExecGrant_common](../E/ExecGrant_common.md) (common grant processing logic)
  - Event trigger functions for collecting grant information

## Notes and Other Information
- This structure is defined in src/include/utils/aclchk_internal.h and is primarily used within the access control checking subsystem
- The structure may be modified during processing when `all_privs` is true and `privileges` is ACL_NO_RIGHTS
- Column-level privileges are only supported for table objects and are handled separately from object-level privileges
- The structure is used both for immediate grant/revoke execution and for event trigger processing to collect information about privilege changes
- Care must be taken when the `privileges` field is modified internally based on `all_privs` and object type