# get_rolespec_oid

## Location
[src/backend/utils/adt/acl.c:5471-5509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5471-L5509)

## Overview
Converts a RoleSpec node from the parser into its corresponding role OID, handling various role specification types including current user references.

## Definition

```c
Oid
get_rolespec_oid(const RoleSpec *role, bool missing_ok)
```
## Detailed Description
This function processes RoleSpec nodes produced by the PostgreSQL parser and converts them to their corresponding role OIDs. RoleSpec is a parser node type that can represent different kinds of role specifications beyond just role names, including special keywords like CURRENT_USER, CURRENT_ROLE, and SESSION_USER.

The function handles multiple role specification types:
- ROLESPEC_CSTRING: A regular role name string that requires catalog lookup
- ROLESPEC_CURRENT_ROLE/ROLESPEC_CURRENT_USER: Returns the current user's OID
- ROLESPEC_SESSION_USER: Returns the session user's OID (the user who initiated the connection)
- ROLESPEC_PUBLIC: Explicitly disallowed and throws an error

This design enforces that PUBLIC must be handled separately by calling code, preventing accidental inclusion in contexts where PUBLIC should not be allowed.

## Parameters / Member Variables
- : Pointer to a RoleSpec node containing the role specification
- : Boolean flag controlling error behavior when role lookup fails
  - : Throw ERROR if role doesn't exist
  - : Return InvalidOid silently if role doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - [RoleSpec](../R/RoleSpec.md) (parser node type for role specifications)
  - ROLESPEC_CSTRING (enum value for string role names)
  - ROLESPEC_CURRENT_ROLE (enum value for CURRENT_ROLE keyword)
  - ROLESPEC_CURRENT_USER (enum value for CURRENT_USER keyword) 
  - ROLESPEC_SESSION_USER (enum value for SESSION_USER keyword)
  - ROLESPEC_PUBLIC (enum value for PUBLIC keyword)
  - [get_role_oid](get_role_oid.md) (role name to OID conversion)
  - [GetUserId](../G/GetUserId.md) (returns current user OID)
  - [GetSessionUserId](../G/GetSessionUserId.md) (returns session user OID)
  - Assert (assertion macro)
  - ereport/elog (error reporting functions)
- Called from (representative examples):
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md) (GRANT/REVOKE statement execution)
  - [ExecAlterDefaultPrivilegesStmt](../E/ExecAlterDefaultPrivilegesStmt.md) (ALTER DEFAULT PRIVILEGES)
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md) (ALTER OWNER commands)
  - [CreateUserMapping](../C/CreateUserMapping.md) (foreign server user mappings)
  - [policy_role_list_to_array](../p/policy_role_list_to_array.md) (row-level security policies)
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md) (schema creation)
  - [GrantRole](../G/GrantRole.md) (role granting)
  - [roleSpecsToIds](../r/roleSpecsToIds.md) (batch role specification conversion)

## Notes and Other Information
- PUBLIC is explicitly rejected with an error message "role \"public\" does not exist"
- The distinction between CURRENT_ROLE and CURRENT_USER is maintained for SQL standard compliance, though they typically resolve to the same OID
- SESSION_USER differs from CURRENT_USER when SET ROLE has been used to change the effective user
- The missing_ok parameter only affects ROLESPEC_CSTRING lookups; other role types either succeed or throw errors
- This function is essential for processing DDL commands that accept role specifications in their syntax

## Simplified Source

```c
Oid get_rolespec_oid(const RoleSpec *role, bool missing_ok) {
    Oid oid;

    switch (role->roletype) {
        case ROLESPEC_CSTRING:
            // Regular role name - look it up in catalog
            oid = get_role_oid(role->rolename, missing_ok);
            break;

        case ROLESPEC_CURRENT_ROLE:
        case ROLESPEC_CURRENT_USER:
            // Current effective user
            oid = GetUserId();
            break;

        case ROLESPEC_SESSION_USER:
            // Original login user (before any SET ROLE)
            oid = GetSessionUserId();
            break;

        case ROLESPEC_PUBLIC:
            // PUBLIC is not allowed - must be handled separately
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("role \"%s\" does not exist", "public")));
            oid = InvalidOid; // Never reached
            break;

        default:
            elog(ERROR, "unexpected role type %d", role->roletype);
    }

    return oid;
}
```