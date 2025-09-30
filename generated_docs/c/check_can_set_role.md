# check_can_set_role

## Location
[src/backend/utils/adt/acl.c:5208-5230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5208-L5230)

## Overview
A validation wrapper that raises a permission violation error if a member role cannot use SET ROLE to assume a target role.

## Definition
```c
void
check_can_set_role(Oid member, Oid role)
```

## Detailed Description
This function serves as an enforcement wrapper around member_can_set_role(), converting a boolean permission check into an error-throwing validation. It provides a standardized way to ensure SET ROLE permissions are met before proceeding with operations that require role assumption.

When the permission check fails, it generates a standardized error message with ERRCODE_INSUFFICIENT_PRIVILEGE, including the name of the target role in the error text for better user feedback.

This function is commonly used in ownership transfer operations and other administrative commands where the current user must be able to assume the target role's identity.

## Parameters / Member Variables
- `member`: The OID of the role whose SET ROLE capability is being validated
- `role`: The OID of the target role that the member must be able to assume

## Dependencies
- Functions called/Symbols referenced:
  - [member_can_set_role](../m/member_can_set_role.md)
  - [GetUserNameFromId](../G/GetUserNameFromId.md)
  - ereport (implicitly through ERROR)
  - ERRCODE_INSUFFICIENT_PRIVILEGE (error code constant)
- Called from:
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md)
  - [createdb](createdb.md)
  - [AlterDatabaseOwner](../A/AlterDatabaseOwner.md)
  - [AlterForeignServerOwner_internal](../A/AlterForeignServerOwner_internal.md)
  - [AlterPublicationOwner_internal](../A/AlterPublicationOwner_internal.md)
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md)
  - [AlterSchemaOwner_internal](../A/AlterSchemaOwner_internal.md)
  - [AlterSubscriptionOwner_internal](../A/AlterSubscriptionOwner_internal.md)
  - [ATExecChangeOwner](../A/ATExecChangeOwner.md)
  - [AlterTypeOwner](../A/AlterTypeOwner.md)

## Notes and Other Information
- Does not return a value - either succeeds silently or raises an ERROR
- Provides consistent error messaging across PostgreSQL's ownership validation
- Typically used in DDL commands that change object ownership
- Error includes the target role name for better debugging and user feedback
- Part of PostgreSQL's defensive programming pattern for permission validation

## Simplified Source

```c
void check_can_set_role(Oid member, Oid role)
{
    // Check if member can assume the target role
    if (!member_can_set_role(member, role)) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be able to SET ROLE \"%s\"",
                        GetUserNameFromId(role, false))));
    }
}
```