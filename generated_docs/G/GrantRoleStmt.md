# GrantRoleStmt

## Location
[src/include/nodes/parsenodes.h:2556-2565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2556-L2565)

## Overview
GrantRoleStmt is a parse tree node structure that represents SQL GRANT ROLE and REVOKE ROLE statements, handling role membership grants and revocations.

## Definition
```c
typedef struct GrantRoleStmt
{
    NodeTag      type;
    List        *granted_roles;    /* list of roles to be granted/revoked */
    List        *grantee_roles;    /* list of member roles to add/delete */
    bool         is_grant;         /* true = GRANT, false = REVOKE */
    List        *opt;              /* options e.g. WITH GRANT OPTION */
    RoleSpec    *grantor;          /* set grantor to other than current role */
    DropBehavior behavior;         /* drop behavior (for REVOKE) */
} GrantRoleStmt;
```

## Detailed Description
GrantRoleStmt represents the parsed form of GRANT ROLE and REVOKE ROLE SQL statements. These statements manage role membership by granting or revoking roles to/from other roles or users. The structure handles the parsing ambiguity with regular GRANT <privileges> statements by storing granted_roles as a list of AccessPriv nodes, though the execution code will reject any column lists that appear.

The statement supports various options such as WITH GRANT OPTION for delegating grant privileges, custom grantors, and different drop behaviors for revocation operations.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a GrantRoleStmt node in the parse tree
- `granted_roles`: List of AccessPriv nodes representing the roles to be granted or revoked (column lists should be empty)
- `grantee_roles`: List of String values representing the member roles that will receive or lose the granted roles
- `is_grant`: Boolean flag indicating the operation type (true for GRANT, false for REVOKE)
- `opt`: List of options such as WITH GRANT OPTION, WITH ADMIN OPTION, etc.
- `grantor`: RoleSpec specifying an alternative grantor (different from the current role executing the statement)
- `behavior`: DropBehavior enum value specifying how to handle dependent objects during REVOKE operations

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree node identification)
  - [List](../L/List.md) (PostgreSQL list data structure)
  - [RoleSpec](../R/RoleSpec.md) (role specification structure)
  - DropBehavior (enumeration for drop behavior)
  - [AccessPriv](../A/AccessPriv.md) (privilege specification structure)

- Called from (representative examples):
  - [GrantRole](GrantRole.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [CreateCommandTag](../C/CreateCommandTag.md)

## Notes and Other Information
- Due to SQL grammar ambiguity, granted_roles uses AccessPriv structures but column specifications are not allowed
- The structure supports both role grants and revocations through the is_grant boolean flag
- Options like WITH GRANT OPTION allow delegation of grant privileges to the grantee
- The grantor field enables privilege escalation scenarios where a superuser grants roles on behalf of another role
- DropBehavior is particularly important for REVOKE operations when cascading effects need to be considered