# has_privs_of_role

## Location
[src/backend/utils/adt/acl.c:5151-5184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5151-L5184)

## Overview
Determines whether a member role has the privileges of a target role, either directly or through inherited role grants.

## Definition
```c
bool
has_privs_of_role(Oid member, Oid role)
```

## Detailed Description
This function checks if a member role possesses the privileges of another role through the role membership hierarchy. It only considers inherited grants, meaning it recurses only through role grants that have the inherit_option set to true.

The function implements several optimization shortcuts:
1. Identity check: if member equals role, immediately returns true
2. Superuser check: superusers automatically have privileges of all roles
3. Full membership traversal: uses roles_is_member_of() with ROLERECURSE_PRIVS to find all roles whose privileges the member inherits

This is distinct from member_can_set_role(), which checks SET ROLE capability rather than privilege inheritance.

## Parameters / Member Variables
- `member`: The OID of the role whose privileges are being checked
- `role`: The OID of the target role whose privileges are being tested for

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - [roles_is_member_of](../r/roles_is_member_of.md)
  - [list_member_oid](../l/list_member_oid.md)
  - ROLERECURSE_PRIVS (constant)
- Called from (representative examples):
  - [ExecAlterDefaultPrivilegesStmt](../E/ExecAlterDefaultPrivilegesStmt.md)
  - [pg_class_aclmask_ext](../p/pg_class_aclmask_ext.md)
  - [pg_namespace_aclmask_ext](../p/pg_namespace_aclmask_ext.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [DoCopy](../D/DoCopy.md)
  - [check_role_grantor](../c/check_role_grantor.md)
  - [aclmask](../a/aclmask.md)
  - [pg_role_aclcheck](../p/pg_role_aclcheck.md)

## Notes and Other Information
- Only considers inherited grants, not SET grants or direct memberships without inheritance
- Superusers automatically pass all privilege checks regardless of actual role memberships  
- Used extensively in permission checking throughout PostgreSQL's access control system
- Complements member_can_set_role() which handles SET ROLE permissions separately
- Returns boolean result making it suitable for direct use in conditional statements