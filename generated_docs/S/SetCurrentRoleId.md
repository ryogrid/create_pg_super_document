# SetCurrentRoleId

## Location
[src/backend/utils/init/miscinit.c:1002-1033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1002-L1033)

## Overview
SetCurrentRoleId changes the current role ID during runtime, implementing the backend logic for PostgreSQL's SET ROLE command.

## Definition


## Detailed Description
This function changes the role ID while PostgreSQL is running, implementing the SET ROLE functionality. It handles two main scenarios: when roleid is InvalidOid (equivalent to 'SET ROLE NONE'), it reverts to the session user authorization; when roleid is valid, it sets the specified role as active. The function is designed to work correctly even in failed transactions to restore prior ROLE GUC variable values. It updates the global SetRoleIsActive flag and delegates the actual user ID change to SetOuterUserId. The function includes safeguards for early GUC initialization when SessionUserId hasn't been set yet.

## Parameters / Member Variables
- : The Oid of the role to set as current, or InvalidOid for 'SET ROLE NONE'
- : Boolean flag indicating whether the specified role has superuser privileges (ignored when roleid is InvalidOid)

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro for checking valid Oids)
  - [SetOuterUserId](SetOuterUserId.md)
  - SetRoleIsActive (global variable assignment)
  - SessionUserId (global variable access)
  - SessionUserIsSuperuser (global variable access)
- Called from (representative examples):
  - [assign_role](../a/assign_role.md) (in variable command processing)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (during parallel worker initialization)
  - [InitializeSessionUserIdStandalone](../I/InitializeSessionUserIdStandalone.md) (during standalone initialization)

## Notes and Other Information
- Caller must verify role membership permissions before calling this function
- Handles 'SET ROLE NONE' by reverting to session user authorization
- Works correctly during GUC initialization and transaction rollback scenarios
- Updates global role state variables to maintain consistency
- Critical component of PostgreSQL's role-based access control system
- The is_superuser parameter is ignored when performing 'SET ROLE NONE'