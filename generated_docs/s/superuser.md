# superuser

## Location
src/backend/utils/misc/superuser.c: 46 - 55

## Overview
Checks whether the current user has PostgreSQL superuser privileges by delegating to `superuser_arg` with the current user ID.

## Definition
```c
bool superuser(void)
```

## Detailed Description
The `superuser` function is a simple wrapper that checks if the currently logged-in PostgreSQL user has superuser privileges. It accomplishes this by calling `superuser_arg` with the result of `GetUserId()`, which returns the OID of the current user. This function is widely used throughout the PostgreSQL codebase to enforce superuser-only operations.

The function provides a convenient interface for checking superuser status without needing to explicitly pass the current user's OID, making the code more readable and reducing the chance of errors.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - `superuser_arg`: The core function that performs the actual superuser check
  - `GetUserId()`: Implicitly called by `superuser_arg` to get the current user OID

- Called from (representative examples):
  - `pg_nextoid`: System function operations
  - `CreateFunction`: Function creation commands  
  - `CreateRole`: Role management commands
  - `AlterRole`: Role modification commands
  - `CreateTableSpace`: Tablespace operations
  - `InitPostgres`: Database initialization
  - `standard_ProcessUtility`: Utility command processing
  - Various DDL commands and administrative functions

## Notes and Other Information
- This function is one of the most frequently called security check functions in PostgreSQL
- It is used to gate access to privileged operations like creating/dropping roles, creating tablespaces, and modifying system catalogs
- The actual superuser determination logic is implemented in `superuser_arg`, which includes caching for performance
- Located in `src/backend/utils/misc/superuser.c:46-55`