# check_session_authorization

## Location
[src/backend/commands/variable.c:802-898](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L802-L898)

## Overview
A GUC check hook function that validates session authorization changes, verifying that the specified role exists and that the current user has permission to assume that role.

## Definition
```c
bool check_session_authorization(char **newval, void **extra, GucSource source)
```

## Detailed Description
The `check_session_authorization` function serves as a validation hook for PostgreSQL's session_authorization configuration parameter, which controls the identity used for permission checks during a session. This function is called whenever SET SESSION AUTHORIZATION is executed or when the parameter is set through other mechanisms.

The function performs comprehensive validation:
1. **Null Handling**: Accepts NULL values (the default boot_val)
2. **Parallel Worker Support**: For parallel workers, it uses the current session state without catalog lookups for consistency with the leader process
3. **Transaction State Check**: Requires an active transaction for catalog lookups, preventing configuration file settings
4. **Role Existence Validation**: Looks up the role in pg_authid system catalog using the provided username
5. **Permission Verification**: Ensures only superusers can change session authorization to a different user than their authenticated identity
6. **Test Mode Handling**: Provides gentler error reporting (NOTICE instead of ERROR) when source is PGC_S_TEST
7. **Data Preparation**: Allocates and populates a role_auth_extra structure with role OID and superuser status for the assign hook

The function handles security carefully by checking the original authenticated user's privileges rather than the current session user's privileges, preventing privilege escalation chains.

## Parameters / Member Variables
- `newval`: Pointer to the proposed new session authorization username string
- `extra`: Pointer to store role_auth_extra structure containing role OID and superuser status
- `source`: The source of the configuration change (affects error handling behavior)

## Dependencies
- Functions called/Symbols referenced:
  - [GetSessionUserId](../G/GetSessionUserId.md)
  - [GetSessionUserIsSuperuser](../G/GetSessionUserIsSuperuser.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - ereport
  - GUC_check_errmsg
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [GetAuthenticatedUserId](../G/GetAuthenticatedUserId.md)
  - superuser_arg
  - GUC_check_errcode
  - [guc_malloc](../g/guc_malloc.md)
- Called from (representative examples):
  - GUC system when processing SET SESSION AUTHORIZATION commands
  - Configuration validation during parameter changes

## Notes and Other Information
- This function is part of a pair of GUC hooks for session_authorization: check_session_authorization and assign_session_authorization
- Uses the role_auth_extra structure to pass role information to the assign hook
- Prevents setting session_authorization in postgresql.conf by requiring transaction state
- Implements different error reporting modes based on the source parameter (TEST vs normal)
- Parallel workers use cached state from the leader process rather than performing fresh catalog lookups
- Security model prevents users from impersonating others unless they are superusers
- The original authenticated user's privileges are checked, not the current session user's privileges
- Located in src/backend/commands/variable.c alongside other authorization-related functions
- Memory allocated for role_auth_extra is automatically freed by the GUC system