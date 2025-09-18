# GetCurrentRoleId

## Location
[src/backend/utils/init/miscinit.c:981-1001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L981-L1001)

## Overview
GetCurrentRoleId returns the current role ID following SET ROLE semantics, providing the outer-level ID rather than the effective ID.

## Definition


## Detailed Description
This function reports the current role ID following the semantics of PostgreSQL's SET ROLE command. It returns the outer-level user ID rather than the current effective ID, and returns InvalidOid when the setting is logically equivalent to 'SET ROLE NONE'. The function checks the global SetRoleIsActive flag to determine whether a role is currently active. If a role is active, it returns the OuterUserId; otherwise, it returns InvalidOid to indicate no role is set.

## Parameters / Member Variables
- This function takes no parameters and returns an Oid value

## Dependencies
- Functions called/Symbols referenced:
  - SetRoleIsActive (global variable check)
  - OuterUserId (global variable access)
  - InvalidOid (constant return value)
- Called from (representative examples):
  - [check_role](../c/check_role.md) (in variable command processing)
  - [show_role](../s/show_role.md) (for displaying current role)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (during parallel query setup)

## Notes and Other Information
- Returns InvalidOid when no role is currently active (equivalent to SET ROLE NONE)
- Used primarily in role management and display functions
- Part of PostgreSQL's role-based access control system
- The function distinguishes between session authorization and current role
- Critical for parallel query processing to maintain proper role context