# GetSessionUserIsSuperuser

## Location
src/backend/utils/init/miscinit.c: 561 - 567

## Overview
GetSessionUserIsSuperuser returns whether the current session user has superuser privileges, providing session-level superuser status information.

## Definition
bool GetSessionUserIsSuperuser(void)

## Detailed Description
GetSessionUserIsSuperuser retrieves the superuser status of the session user stored in the SessionUserIsSuperuser static variable. This boolean value indicates whether the user associated with the current session (as returned by GetSessionUserId) has superuser privileges in the database cluster.

The session user superuser status is established when the session is created and can be modified by SET SESSION AUTHORIZATION commands (which can only be executed by superusers). This status is distinct from the current effective superuser status, which may change during role switches or SECURITY DEFINER function executions.

## Parameters / Member Variables
This function takes no parameters and returns:
- Return value: bool - True if session user is a superuser, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
  - OidIsValid (macro to validate SessionUserId)
  - SessionUserId (static variable for session validation)
  - SessionUserIsSuperuser (static variable holding superuser status)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (for parallel worker session setup)
  - [check_session_authorization](../c/check_session_authorization.md) (for GUC variable validation)
  - AmSpecialWorkerProcess (for worker process role checking)

## Notes and Other Information
- This function is paired with GetSessionUserId to provide complete session user identity information
- The superuser status is cached and synchronized with the session user ID via SetSessionUserId
- Used for determining whether SET SESSION AUTHORIZATION commands are permitted
- Essential for parallel query execution where worker processes need to inherit session context
- The assertion ensures the function is only called after proper session initialization
- [Session](../S/Session.md) superuser status persists across SET ROLE operations, unlike current effective privileges
- Critical for session-level authorization decisions and security policy enforcement