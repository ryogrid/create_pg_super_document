# GetAuthenticatedUserId

## Location
[src/backend/utils/init/miscinit.c:591-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L591-L597)

## Overview
Returns the authenticated user ID (OID) that was established during connection authentication and remains constant throughout the entire session lifetime.

## Definition

```c
Oid
GetAuthenticatedUserId(void)
```
## Detailed Description
GetAuthenticatedUserId is a simple accessor function that returns the OID of the user who was authenticated when the database connection was first established. This user ID represents the original authenticated identity and never changes during the session, regardless of any subsequent SET SESSION AUTHORIZATION or SET ROLE commands.

The authenticated user ID is distinct from both the session user ID (which can be changed via SET SESSION AUTHORIZATION) and the current user ID (which can be changed via SET ROLE). It serves as an immutable record of who originally authenticated to establish the connection.

The function includes an assertion to ensure that the AuthenticatedUserId has been properly initialized with a valid OID before being accessed.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for validation)
  - OidIsValid (to validate AuthenticatedUserId)
  - AuthenticatedUserId (global static variable access)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (in parallel.c:340)
  - [check_session_authorization](../c/check_session_authorization.md) (in variable.c:869, 870)
  - External functions via miscadmin.h header inclusion

## Notes and Other Information
- The AuthenticatedUserId is set exactly once during connection establishment via SetAuthenticatedUserId()
- This ID remains constant throughout the entire session, providing an audit trail of the original authentication
- Used for security checks, parallel query initialization, and session authorization validation
- The function is part of PostgreSQL's three-tier user identity system (authenticated, session, and current user)
- Unlike SessionUserId or current user ID, this value cannot be changed by SQL commands
- Critical for maintaining security context in scenarios involving role switching or privilege escalation