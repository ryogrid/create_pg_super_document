# has_createrole_privilege

## Location
[src/backend/catalog/aclchk.c:4228-4246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4228-L4246)

## Overview
Checks whether a specified role has CREATEROLE privilege or is a superuser, providing an ownership-like permissions test for role-related operations.

## Definition
```c
bool has_createrole_privilege(Oid roleid)
```

## Detailed Description
This function determines if a role has the CREATEROLE privilege, which allows creating, altering, and dropping other roles. It serves as an ownership-like permission check for role operations since roles don't have owners in the traditional sense. The function first checks if the role is a superuser (which automatically grants all privileges), then examines the `rolcreaterole` attribute in the pg_authid system catalog. The function is typically applied to the role performing the operation, not the target role being operated on.

## Parameters / Member Variables
- `roleid`: The OID of the role whose CREATEROLE privilege is being checked

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - Form_pg_authid
- Called from (representative examples):
  - [check_object_ownership](../c/check_object_ownership.md)
  - [have_createrole_privilege](have_createrole_privilege.md)
  - [CreateRole](../C/CreateRole.md)

## Notes and Other Information
- Located in src/backend/catalog/aclchk.c:4228-4246
- Returns true if the role has CREATEROLE privilege or is a superuser, false otherwise
- Used as an ownership-like test since roles don't have traditional owners
- Should be applied to the role performing the operation, not the target role
- Caller should handle additional checks if the target role is a superuser
- The CREATEROLE privilege is stored in the `rolcreaterole` field of pg_authid
- Essential for PostgreSQL's role-based access control system
- Does not automatically grant permission to modify superuser roles - additional checks are needed