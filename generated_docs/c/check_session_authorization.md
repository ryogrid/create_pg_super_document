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
  - [superuser_arg](../s/superuser_arg.md)
  - [GUC_check_errcode](../G/GUC_check_errcode.md)
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

## Simplified Source

```c
bool check_session_authorization(char **newval, void **extra, GucSource source)
{
    HeapTuple roleTup;
    Form_pg_authid roleform;
    Oid roleid;
    bool is_superuser;
    role_auth_extra *myextra;

    // Handle NULL (default value)
    if (*newval == NULL)
        return true;

    // Special handling for parallel workers
    if (InitializingParallelWorker) {
        roleid = GetSessionUserId();
        is_superuser = GetSessionUserIsSuperuser();
    } else {
        // Require transaction state for catalog lookups
        if (!IsTransactionState())
            return false;

        // Look up the role
        roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(*newval));
        if (!HeapTupleIsValid(roleTup)) {
            if (source == PGC_S_TEST) {
                ereport(NOTICE, (errcode(ERRCODE_UNDEFINED_OBJECT),
                               errmsg("role \"%s\" does not exist", *newval)));
                return true;
            }
            GUC_check_errmsg("role \"%s\" does not exist", *newval);
            return false;
        }

        roleform = (Form_pg_authid) GETSTRUCT(roleTup);
        roleid = roleform->oid;
        is_superuser = roleform->rolsuper;
        ReleaseSysCache(roleTup);

        // Check permissions (only superusers can change to different user)
        if (roleid != GetAuthenticatedUserId() && !superuser_arg(GetAuthenticatedUserId())) {
            if (source == PGC_S_TEST) {
                ereport(NOTICE, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                               errmsg("permission will be denied to set session authorization \"%s\"", *newval)));
                return true;
            }
            GUC_check_errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg("permission denied to set session authorization \"%s\"", *newval);
            return false;
        }
    }

    // Store role info for assign hook
    myextra = (role_auth_extra *) guc_malloc(LOG, sizeof(role_auth_extra));
    if (!myextra)
        return false;
    myextra->roleid = roleid;
    myextra->is_superuser = is_superuser;
    *extra = (void *) myextra;

    return true;
}
```