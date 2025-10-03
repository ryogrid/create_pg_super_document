# SetCurrentRoleId

## Location
[src/backend/utils/init/miscinit.c:1002-1033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1002-L1033)

## Overview
SetCurrentRoleId changes the current role ID during runtime, implementing the backend logic for PostgreSQL's SET ROLE command.

## Definition

```c
void
SetCurrentRoleId(Oid roleid, bool is_superuser)
```
## Detailed Description
This function changes the role ID while PostgreSQL is running, implementing the SET ROLE functionality. It handles two main scenarios: when roleid is InvalidOid (equivalent to 'SET ROLE NONE'), it reverts to the session user authorization; when roleid is valid, it sets the specified role as active. The function is designed to work correctly even in failed transactions to restore prior ROLE GUC variable values. It updates the global SetRoleIsActive flag and delegates the actual user ID change to SetOuterUserId. The function includes safeguards for early GUC initialization when SessionUserId hasn't been set yet.

## Parameters / Member Variables
- `roleid`: The Oid of the role to set as current, or InvalidOid for 'SET ROLE NONE'
- `is_superuser`: Boolean flag indicating whether the specified role has superuser privileges (ignored when roleid is InvalidOid)
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

## Simplified Source

```c
// Simplified version of SetCurrentRoleId
void SetCurrentRoleId(Oid roleid, bool is_superuser) {
    if (!OidIsValid(roleid)) {
        // SET ROLE NONE: revert to session user
        SetRoleIsActive = false;

        // Early return if session not yet initialized
        if (!OidIsValid(SessionUserId))
            return;

        // Use session user credentials
        roleid = SessionUserId;
        is_superuser = SessionUserIsSuperuser;
    } else {
        // SET ROLE <roleid>: activate the specified role
        SetRoleIsActive = true;
    }

    // Apply the role change
    SetOuterUserId(roleid, is_superuser);
}
```

Key simplifications made:
- Clarified the two main branches: SET ROLE NONE vs SET ROLE <roleid>
- Added explanatory comments for each decision point
- Emphasized the role activation state management
- Simplified the early initialization handling logic