# check_can_set_role

## Location
src/backend/utils/adt/acl.c: 5208 - 5230

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
  - member_can_set_role
  - GetUserNameFromId
  - ereport (implicitly through ERROR)
  - ERRCODE_INSUFFICIENT_PRIVILEGE (error code constant)
- Called from:
  - AlterObjectOwner_internal
  - createdb
  - AlterDatabaseOwner
  - AlterForeignServerOwner_internal
  - AlterPublicationOwner_internal
  - CreateSchemaCommand
  - AlterSchemaOwner_internal
  - AlterSubscriptionOwner_internal
  - ATExecChangeOwner
  - AlterTypeOwner

## Notes and Other Information
- Does not return a value - either succeeds silently or raises an ERROR
- Provides consistent error messaging across PostgreSQL's ownership validation
- Typically used in DDL commands that change object ownership
- Error includes the target role name for better debugging and user feedback
- Part of PostgreSQL's defensive programming pattern for permission validation