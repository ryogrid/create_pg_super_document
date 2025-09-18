# RemoveRoleFromObjectACL

## Location
src/backend/catalog/aclchk.c: 1466 - 1600

## Overview
Removes all mentions of a role from an object's Access Control List (ACL), used when dropping a role to clean up all associated permissions.

## Definition
```c
void RemoveRoleFromObjectACL(Oid roleid, Oid classid, Oid objid)
```

## Detailed Description
This function is used by `shdepDropOwned` to remove mentions of a role in ACLs when a role is being dropped from the system. It handles two main cases:

1. **Default ACLs** (when classid == DefaultAclRelationId): The function retrieves the default ACL information from pg_default_acl, constructs an InternalDefaultACL structure, and calls SetDefaultACL to revoke all privileges.

2. **Regular Object ACLs**: For other object types, it maps the object class ID to the appropriate object type, constructs an InternalGrant structure, and calls ExecGrantStmt_oids to perform a REVOKE ALL operation.

The function effectively performs a "REVOKE ALL" operation on the specified object for the given role. For table objects, this also revokes any column-level permissions per SQL standard behavior.

## Parameters
- `roleid`: The OID of the role to be removed from the ACL
- `classid`: The system catalog relation OID that defines the object type (e.g., RelationRelationId for tables)
- `objid`: The OID of the specific object whose ACL should be modified

## Dependencies
- Functions called/Symbols referenced:
  - [SetDefaultACL](../S/SetDefaultACL.md)
  - [ExecGrantStmt_oids](../E/ExecGrantStmt_oids.md)
  - table_open
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - list_make1_oid
- Called from (representative examples):
  - [shdepDropOwned](../s/shdepDropOwned.md)

## Notes and Other Information
- The function does not accept an objsubid parameter, which means it operates at the object level rather than sub-object level (like specific columns)
- For table objects with column-level permissions, the function issues REVOKE ALL ON TABLE which also revokes column permissions according to SQL specification
- This is designed for role deletion scenarios where all permissions must be removed
- The function handles various PostgreSQL object types including tables, databases, types, procedures, languages, large objects, schemas, tablespaces, foreign servers, foreign data wrappers, and parameter ACLs