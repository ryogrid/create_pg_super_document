# assign_role

## Location
[src/backend/commands/variable.c:1014-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1014-L1021)

## Overview
The `assign_role` function completes the SET ROLE operation by actually changing the current role identity in the PostgreSQL session.

## Definition
```c
void assign_role(const char *newval, void *extra)
```

## Detailed Description
This function is a GUC (Grand Unified Configuration) assign hook that performs the actual role change after validation by `check_role`. It serves as the final step in the SET ROLE command execution sequence. The function is deliberately simple and focused, extracting the validated role information from the extra data structure and applying it to the current session using the session management infrastructure.

The function works in conjunction with `check_role` which performs all the heavy lifting of validation, authentication, and privilege checking. By the time `assign_role` is called, all necessary checks have been completed and the role change is guaranteed to be valid and authorized.

## Parameters / Member Variables
- `newval`: The role name string (not directly used by this function, included for GUC hook interface compliance)
- `extra`: Pointer to role_auth_extra structure containing validated role OID and superuser status, prepared by check_role

## Dependencies
- Functions called/Symbols referenced:
  - [SetCurrentRoleId](../S/SetCurrentRoleId.md)
  - role_auth_extra (data structure)
- Called from (representative examples):
  - GUC system framework (as assign hook)

## Notes and Other Information
- Always called after successful validation by check_role function
- Part of the GUC hook mechanism for configuration changes
- Implements the final step of PostgreSQL's role-based access control system
- Works with the session management system to maintain role identity
- Simple implementation reflects that all complex validation is handled in the check phase
- Critical for maintaining security context throughout the database session