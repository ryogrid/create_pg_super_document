# SetSessionAuthorization

## Location
src/backend/utils/init/miscinit.c: 966 - 980

## Overview
SetSessionAuthorization changes the session authorization user ID during runtime, implementing the backend logic for PostgreSQL's SET SESSION AUTHORIZATION command.

## Definition


## Detailed Description
This function changes the session authorization user ID while PostgreSQL is running. It serves as the GUC (Grand Unified Configuration) assign hook for the "session_authorization" parameter. The function implements the SQL standard requirement that SET SESSION AUTHORIZATION implies SET ROLE NONE. The function is designed to be commutative with SetCurrentRoleId because guc.c provides no guarantees about which hook will run first during operations like transaction rollback. It updates the session user ID and conditionally updates the outer user ID only when no role is currently active (!SetRoleIsActive).

## Parameters / Member Variables
- : The Oid of the user to set as the new session authorization
- : Boolean flag indicating whether the specified user has superuser privileges

## Dependencies
- Functions called/Symbols referenced:
  - [SetSessionUserId](SetSessionUserId.md)
  - [SetOuterUserId](SetOuterUserId.md)
  - SetRoleIsActive (global variable check)
- Called from (representative examples):
  - [assign_session_authorization](../a/assign_session_authorization.md) (in variable command processing)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (during parallel worker initialization)
  - [InitializeSessionUserIdStandalone](../I/InitializeSessionUserIdStandalone.md) (during standalone initialization)

## Notes and Other Information
- This function is part of PostgreSQL's role and authorization management system
- It's designed to work correctly with PostgreSQL's GUC system and transaction rollback mechanisms
- The conditional update of OuterUserId ensures proper interaction with SET ROLE functionality
- Used primarily in session management and parallel worker processes
- Must maintain consistency between session authorization and role state