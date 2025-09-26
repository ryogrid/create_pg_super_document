# check_rolespec_name

## Location
[src/backend/utils/adt/acl.c:5578-5600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5578-L5600)

## Overview
Validates that a RoleSpec's name is not a reserved PostgreSQL role name, throwing an error with optional detail message if it is reserved.

## Definition
```c
void check_rolespec_name(const RoleSpec *role, const char *detail_msg)
```

## Detailed Description
This function performs validation on role names to ensure they do not conflict with PostgreSQL's reserved names. It accepts a RoleSpec pointer and an optional detail message, then checks if the role name is reserved using IsReservedName(). If a reserved name is detected, the function throws an ERROR with appropriate error codes and messages.

The function includes several safety checks: it returns early if the role pointer is NULL or if the role type is not ROLESPEC_CSTRING (meaning it's not a string-based role specification). Only string-based role specifications are validated against the reserved names list.

The error reporting includes both a standard error message indicating the role name is reserved, and optionally includes additional detail information if provided by the caller.

## Parameters / Member Variables
- `role`: Pointer to a RoleSpec structure containing the role specification to validate (can be NULL)
- `detail_msg`: Optional detail message to include in error reports if validation fails (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - IsReservedName
  - ereport
  - errcode
  - errmsg
  - errdetail_internal
  - ROLESPEC_CSTRING (enum value)
- Called from (representative examples):
  - AlterRole (in src/backend/commands/user.c)
  - AlterRoleSet (in src/backend/commands/user.c)

## Notes and Other Information
- Function is designed to be safe with NULL inputs - no error thrown if role is NULL
- Only validates ROLESPEC_CSTRING type role specifications
- Uses ERRCODE_RESERVED_NAME error code for consistent error handling
- The detail_msg parameter must already be translated if provided
- Part of PostgreSQL's role management and security validation system
- Located in src/backend/utils/adt/acl.c:5578-5600