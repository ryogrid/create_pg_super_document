# GetOuterUserId

## Location
[src/backend/utils/init/miscinit.c:526-533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L526-L533)

## Overview
GetOuterUserId returns the current user ID in effect at the "outer level" (outside any transaction or function), representing the current role context for the session.

## Definition
Oid GetOuterUserId(void)

## Detailed Description
GetOuterUserId retrieves the outer-level user ID stored in the OuterUserId static variable. This represents the current user ID at the top level of execution, outside of any SECURITY DEFINER functions or local user context changes. The OuterUserId is initially the same as SessionUserId when a session starts, but can be changed by the SET ROLE command to any role that the session user is a member of.

The outer user ID differs from the current effective user ID (returned by GetUserId) in that it remains stable during SECURITY DEFINER function calls, while the effective user ID may change temporarily. The function includes an assertion to ensure OuterUserId contains a valid OID.

## Parameters / Member Variables
This function takes no parameters and returns:
- Return value: Oid - The outer-level user ID (OuterUserId)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
  - OidIsValid (macro to check if OID is valid)  
  - OuterUserId (static variable holding outer-level user ID)
- Called from (representative examples):
  - [DropRole](../D/DropRole.md) (to check if dropping the current outer role)
  - [RenameRole](../R/RenameRole.md) (to check if renaming the current outer role)
  - AmSpecialWorkerProcess (role context checking)

## Notes and Other Information
- OuterUserId represents the result of SET ROLE commands and is used for role-based access control
- At the outer level, CurrentUserId equals OuterUserId, but they may differ during SECURITY DEFINER function execution
- The outer user ID is updated by SetOuterUserId() when SET ROLE is executed
- This value is used by GetCurrentRoleId() when SET ROLE is active
- OuterUserId is synchronized with the is_superuser GUC parameter
- The naming is somewhat confusing - it might be better named CurrentRoleId as noted in the source comments