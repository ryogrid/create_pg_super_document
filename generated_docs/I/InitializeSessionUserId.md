# InitializeSessionUserId

## Location
[src/backend/utils/init/miscinit.c:756-885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L756-L885)

## Overview
Initializes user identity during normal backend startup by validating the role and setting up session-level authentication parameters.

## Definition

```c
void
InitializeSessionUserId(const char *rolename, Oid roleid,
						bool bypass_login_check)
```
## Detailed Description
This function performs comprehensive user identity initialization for PostgreSQL backend processes. It handles role lookup (by name or OID), validates role existence and login permissions, enforces connection limits, and establishes the authenticated user context. The function includes special handling for parallel workers and bootstrap mode, and implements PostgreSQL's role-based authentication with configurable login bypass for background workers. It also manages the session_authorization GUC variable and performs syscache invalidation to ensure current role information.

## Parameters / Member Variables
- `*rolename`: The name of the role to initialize (can be NULL if roleid is provided)
- `roleid`: The object identifier (Oid) of the role (used when rolename is NULL)
- `bypass_login_check`: Boolean flag to bypass login permission checks (used for background workers)
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_authid (pg_authid catalog structure)
  - IsBootstrapProcessingMode (bootstrap mode check)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (syscache invalidation)
  - [SetAuthenticatedUserId](../S/SetAuthenticatedUserId.md) (sets authenticated user ID)
  - [SetConfigOption](../S/SetConfigOption.md) (sets GUC variables)
  - PGC_BACKEND, PGC_S_OVERRIDE (GUC setting flags)
  - AmRegularBackendProcess (backend process type check)
  - [CountUserBackends](../C/CountUserBackends.md) (connection count function)
  - [SearchSysCache1](../S/SearchSysCache1.md), HeapTupleIsValid, ReleaseSysCache (catalog access functions)
- Called from (representative examples):
  - [InitPostgres](InitPostgres.md) (src/backend/utils/init/postinit.c:917, 927)
  - AmSpecialWorkerProcess (src/include/miscadmin.h:414)

## Notes and Other Information
- Skips execution entirely for parallel workers that have already been initialized
- Asserts that bootstrap mode is not active (catalogs must exist)
- Performs syscache invalidation to find recently created roles
- Handles both role name and role OID based lookups
- Enforces rolcanlogin permission unless bypass_login_check is true
- Implements approximate connection limiting with documented race conditions
- Sets session_authorization GUC with PGC_S_OVERRIDE to prevent later changes
- Connection limits are only enforced for regular backend processes and non-superusers
- Critical part of PostgreSQL's authentication and authorization infrastructure

## Simplified Source

```c
// Simplified version of InitializeSessionUserId
void InitializeSessionUserId(const char *rolename, Oid roleid, bool bypass_login_check) {
    // Skip for parallel workers - already initialized by ParallelWorkerMain
    if (InitializingParallelWorker) {
        Assert(bypass_login_check);
        return;
    }

    // Bootstrap mode should not reach here
    Assert(!IsBootstrapProcessingMode());

    // Refresh syscache to find recently created roles
    AcceptInvalidationMessages();

    // Look up role by name or OID
    HeapTuple roleTup;
    if (rolename != NULL) {
        roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
        if (!HeapTupleIsValid(roleTup))
            ereport(FATAL, (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                           errmsg("role \"%s\" does not exist", rolename)));
    } else {
        roleTup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
        if (!HeapTupleIsValid(roleTup))
            ereport(FATAL, (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                           errmsg("role with OID %u does not exist", roleid)));
    }

    // Extract role information
    Form_pg_authid rform = (Form_pg_authid) GETSTRUCT(roleTup);
    roleid = rform->oid;
    char *rname = NameStr(rform->rolname);
    bool is_superuser = rform->rolsuper;

    // Set authenticated user ID and session authorization
    SetAuthenticatedUserId(roleid);
    SetConfigOption("session_authorization", rname, PGC_BACKEND, PGC_S_OVERRIDE);

    // Enforce login restrictions (only under postmaster)
    if (IsUnderPostmaster) {
        // Check if role can login
        if (!bypass_login_check && !rform->rolcanlogin)
            ereport(FATAL, (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                           errmsg("role \"%s\" is not permitted to log in", rname)));

        // Check connection limit for regular backends
        if (rform->rolconnlimit >= 0 && AmRegularBackendProcess() && !is_superuser &&
            CountUserBackends(roleid) > rform->rolconnlimit)
            ereport(FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                           errmsg("too many connections for role \"%s\"", rname)));
    }

    ReleaseSysCache(roleTup);
}
```

Key simplifications made:
- Removed detailed comments about GUC handling complexity
- Consolidated variable declarations closer to usage
- Simplified role lookup logic flow
- Removed extensive commentary about race conditions
- Focused on the main authentication and authorization logic
- Maintained all critical security checks and error handling