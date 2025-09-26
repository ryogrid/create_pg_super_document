# has_privs_of_role

## Location
src/backend/utils/adt/acl.c: 5151 - 5184

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
  - superuser_arg
  - roles_is_member_of
  - list_member_oid
  - ROLERECURSE_PRIVS (constant)
- Called from (representative examples):
  - ExecAlterDefaultPrivilegesStmt
  - pg_class_aclmask_ext
  - pg_namespace_aclmask_ext
  - object_ownercheck
  - DoCopy
  - check_role_grantor
  - aclmask
  - pg_role_aclcheck

## Notes and Other Information
- Only considers inherited grants, not SET grants or direct memberships without inheritance
- Superusers automatically pass all privilege checks regardless of actual role memberships  
- Used extensively in permission checking throughout PostgreSQL's access control system
- Complements member_can_set_role() which handles SET ROLE permissions separately
- Returns boolean result making it suitable for direct use in conditional statements