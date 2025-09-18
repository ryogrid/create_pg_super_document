# GetSessionUserId

## Location
[src/backend/utils/init/miscinit.c:554-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L554-L560)

## Overview
GetSessionUserId returns the session user ID, which is the user identity established at session start and can be modified by SET SESSION AUTHORIZATION.

## Definition
Oid GetSessionUserId(void)

## Detailed Description
GetSessionUserId retrieves the session user ID stored in the SessionUserId static variable. This represents the user identity associated with the current database session. The session user ID is initially set to the same value as the authenticated user ID when a session begins, but can be changed by superusers using the SET SESSION AUTHORIZATION command.

The session user ID is what is reported by the SESSION_USER SQL function and represents the "session identity" as opposed to the current effective user ID (which may change during SECURITY DEFINER function execution) or the current role (which may change during SET ROLE operations).

## Parameters / Member Variables
This function takes no parameters and returns:
- Return value: Oid - The session user ID (SessionUserId)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
  - OidIsValid (macro to check if OID is valid)
  - SessionUserId (static variable holding session user ID)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (for parallel query session context)
  - [DropRole](../D/DropRole.md)/RenameRole (to check if operating on session user)
  - [check_session_authorization](../c/check_session_authorization.md) (for GUC variable validation)
  - [session_user](../s/session_user.md) (SQL function implementation)
  - [InitPostgres](../I/InitPostgres.md) (during backend initialization)
  - [pgstat_bestart](../p/pgstat_bestart.md) (for statistics reporting)

## Notes and Other Information
- SessionUserId differs from AuthenticatedUserId in that it can be changed by SET SESSION AUTHORIZATION
- SessionUserId differs from OuterUserId/CurrentUserId in that it persists across SET ROLE operations
- Only superusers can use SET SESSION AUTHORIZATION to change the session user ID
- This is the user ID returned by the SESSION_USER SQL function
- The session user ID is used as the base identity for determining which roles can be assumed via SET ROLE
- Related to SessionUserIsSuperuser which tracks superuser status of the session user
- Critical for session-level security and auditing purposes