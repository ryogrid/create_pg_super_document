# SetAuthenticatedUserId

## Location
src/backend/utils/init/miscinit.c: 598 - 657

## Overview
Sets the authenticated user ID for the current session exactly once during connection establishment, also updating the corresponding PGPROC entry to record the authenticated user.

## Definition


## Detailed Description
SetAuthenticatedUserId is a critical function in PostgreSQL's authentication system that establishes the authenticated user identity for a database session. This function can only be called once per session and sets both the global AuthenticatedUserId variable and the roleId field in the current process's PGPROC entry.

The function includes strict assertions to ensure it's used correctly: the provided userid must be valid, and the AuthenticatedUserId must not already be set (ensuring single-call semantics). The PGPROC entry update allows other processes to identify the authenticated user for this backend process, which is important for monitoring, security auditing, and process management.

The authenticated user ID established by this function remains constant throughout the session and serves as the foundation for PostgreSQL's multi-layered security model.

## Parameters / Member Variables
- : The OID of the authenticated user. Must be a valid OID (not InvalidOid) and represents the user who successfully authenticated to establish this connection.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for validation checks)
  - OidIsValid (to validate the userid parameter)
  - AuthenticatedUserId (global static variable)
  - MyProc->roleId (PGPROC structure field access)
- Called from (representative examples):
  - ParallelWorkerMain (in parallel.c:1417)
  - InitializeSessionUserId (in miscinit.c:815)

## Notes and Other Information
- This function enforces single-call semantics - it can only be called once per session
- Updates both the local AuthenticatedUserId variable and the shared PGPROC entry
- The PGPROC update is assumed to be atomic and requires no locking
- Called during initial session setup after successful user authentication
- Critical for establishing the security context that underlies all subsequent authorization decisions
- The authenticated user ID cannot be changed once set, unlike session or current user IDs
- Used in parallel worker processes to inherit the authenticated identity from the main backend process