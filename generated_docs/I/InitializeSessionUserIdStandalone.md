# InitializeSessionUserIdStandalone

## Location
[src/backend/utils/init/miscinit.c:886-919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L886-L919)

## Overview
Initializes user identity during special backend startup for single-user mode, autovacuum workers, slot sync workers, and background workers using the bootstrap superuser.

## Definition

```c
void
InitializeSessionUserIdStandalone(void)
```
## Detailed Description
This function provides a simplified user identity initialization for special PostgreSQL backend processes that don't follow the normal authentication flow. It sets the authenticated user to the bootstrap superuser (BOOTSTRAP_SUPERUSERID) and configures session authorization accordingly. Unlike the normal initialization process, this function bypasses role validation and login checks, making it suitable for system processes that need to operate even when the authentication catalog might be compromised or unavailable.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - AmAutoVacuumWorkerProcess (autovacuum worker check)
  - AmBackgroundWorkerProcess (background worker check)
  - AmLogicalSlotSyncWorkerProcess (slot sync worker check)
  - [SetSessionAuthorization](../S/SetSessionAuthorization.md) (sets session authorization)
  - [SetCurrentRoleId](../S/SetCurrentRoleId.md) (sets current role)
  - BOOTSTRAP_SUPERUSERID (bootstrap superuser constant)
  - InvalidOid (invalid OID constant)
- Called from (representative examples):
  - [InitPostgres](InitPostgres.md) (src/backend/utils/init/postinit.c:894, 899, 912)
  - AmSpecialWorkerProcess (src/include/miscadmin.h:416)

## Notes and Other Information
- Only callable in single-user mode, autovacuum workers, slot sync workers, or background workers
- Asserts that it's called only once per session (AuthenticatedUserId must be invalid)
- Sets AuthenticatedUserId to BOOTSTRAP_SUPERUSERID directly without catalog lookup
- Does not set the session_authorization GUC variable to avoid requiring role name lookup
- Comments indicate this approach allows startup even if the bootstrap superuser's pg_authid row is corrupted
- Sets current role to InvalidOid for consistency with manual role setting
- Critical for system recovery and maintenance operations that must work regardless of catalog state