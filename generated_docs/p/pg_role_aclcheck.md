# pg_role_aclcheck

## Location
[src/backend/utils/adt/acl.c:4877-4906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4877-L4906)

## Overview
This function performs access control checks for role-based privileges by evaluating whether a given role has the requested permissions on a target role using different privilege levels.

## Definition

```c
static AclResult
pg_role_aclcheck(Oid role_oid, Oid roleid, AclMode mode)
```
## Detailed Description
The `pg_role_aclcheck` function is a static helper function that provides "quick-and-dirty" support for the pg_has_role family of functions. It checks various types of role privileges based on the ACL mode flags provided. The function evaluates four types of permissions: admin privileges (using ACL_CREATE with grant option), membership (using ACL_CREATE), usage privileges (ACL_USAGE), and SET privileges (ACL_SET). Each privilege type is checked using corresponding role membership functions, and the function returns ACLCHECK_OK if any requested privilege is satisfied, or ACLCHECK_NO_PRIV if none are met.

## Parameters / Member Variables
- `role_oid` (Oid): The OID of the target role on which privileges are being checked
- `roleid` (Oid): The OID of the user role whose privileges are being evaluated
- `mode` (AclMode): Bitmask representing the types of privileges to check

## Dependencies
- Functions called/Symbols referenced:
  - ACL_GRANT_OPTION_FOR
  - [is_admin_of_role](../i/is_admin_of_role.md)
  - [is_member_of_role](../i/is_member_of_role.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [member_can_set_role](../m/member_can_set_role.md)
  - ACL_CREATE
  - ACL_USAGE
  - ACL_SET
  - ACLCHECK_NO_PRIV
- Called from (representative examples):
  - [pg_has_role_name_name](pg_has_role_name_name.md)
  - [pg_has_role_name](pg_has_role_name.md)
  - [pg_has_role_name_id](pg_has_role_name_id.md)
  - [pg_has_role_id](pg_has_role_id.md)
  - [pg_has_role_id_name](pg_has_role_id_name.md)
  - [pg_has_role_id_id](pg_has_role_id_id.md)

## Notes and Other Information
- Uses ACL_CREATE to represent membership privileges due to lack of dedicated membership ACL bit
- Checks privileges in order: admin privileges, membership, usage, and SET permissions
- Returns ACLCHECK_OK on first successful privilege match, providing short-circuit evaluation
- Function is static, indicating internal use within the ACL subsystem
- Part of PostgreSQL's role-based access control system supporting hierarchical role privileges