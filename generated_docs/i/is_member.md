# is_member

## Location
[src/backend/libpq/hba.c:919-947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L919-L947)

## Overview
Determines whether a given user (by OID) belongs to a specified role, used in PostgreSQL's authentication and authorization system.

## Definition
```c
static bool is_member(Oid userid, const char *role)
```

## Detailed Description
The is_member function is a utility function used during authentication to determine if a user belongs to a specific role. This is essential for role-based authentication rules in pg_hba.conf where authentication entries can specify role names prefixed with '+' to indicate group membership requirements.

The function performs several key operations:

1. **Input Validation**: Checks if the provided user OID is valid
2. **Role Resolution**: Converts the role name string to its corresponding OID using get_role_oid()
3. **Membership Check**: Uses is_member_of_role_nosuper() to determine if the user is a direct or indirect member of the target role

Importantly, this function explicitly excludes superuser privileges from automatic role membership - superusers are not automatically considered members of all roles for the purpose of authentication group checks.

## Parameters
- `userid`: The OID of the user/role attempting to authenticate
- `role`: The name of the role to check membership against (as a string)

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid](../g/get_role_oid.md)
  - [is_member_of_role_nosuper](is_member_of_role_nosuper.md)
  - OidIsValid (macro)
- Called from (representative examples):
  - [check_role](../c/check_role.md)
  - [check_db](../c/check_db.md)

## Notes and Other Information
- This is a static function, accessible only within the hba.c file
- Returns false if either the user OID or role name is invalid/non-existent
- Superusers are explicitly not considered automatic members of all roles for authentication purposes
- Used primarily in the context of processing '+' prefixed role specifications in pg_hba.conf entries
- Part of PostgreSQL's Host-Based Authentication (HBA) system for controlling database access
- The function distinguishes between direct role membership and inherited membership through role hierarchies

## Simplified Source
```c
static bool
is_member(Oid userid, const char *role)
{
    // Validate user OID
    if (!OidIsValid(userid))
        return false;

    // Look up the role OID from the role name
    Oid roleid = get_role_oid(role, true);
    if (!OidIsValid(roleid))
        return false;

    // Check if user is a member of the role (excluding superuser privilege)
    return is_member_of_role_nosuper(userid, roleid);
}
```