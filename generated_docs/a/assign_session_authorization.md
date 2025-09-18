# assign_session_authorization

## Location
[src/backend/commands/variable.c:899-920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L899-L920)

## Overview
A GUC assign hook function that actually implements a session authorization change after validation, using role information prepared by the check hook.

## Definition
```c
void assign_session_authorization(const char *newval, void *extra)
```

## Detailed Description
The `assign_session_authorization` function is the assign hook for PostgreSQL's session_authorization configuration parameter. It is called after `check_session_authorization` has successfully validated the session authorization change and is responsible for actually implementing the authorization change in the current session.

The function is deliberately simple and performs only the essential operation:
1. **Null Handling**: Returns immediately if extra is NULL, which occurs for the boot_val default
2. **Authorization Application**: Calls `SetSessionAuthorization` with the role OID and superuser status that were validated and stored by the check hook
3. **Data Extraction**: Retrieves role information from the role_auth_extra structure passed via the extra parameter

The function relies entirely on the validation performed by `check_session_authorization` and assumes that if the check hook succeeded, the authorization change is safe and valid to apply.

## Parameters / Member Variables
- `newval`: The username string (not used in this function since role information comes from extra)
- `extra`: Pointer to role_auth_extra structure containing validated role OID and superuser status

## Dependencies
- Functions called/Symbols referenced:
  - [SetSessionAuthorization](../S/SetSessionAuthorization.md)
  - role_auth_extra (structure type)
- Called from (representative examples):
  - GUC system after successful validation by check_session_authorization
  - SET SESSION AUTHORIZATION command execution
  - Role-related configuration changes

## Notes and Other Information
- This function is part of a pair of GUC hooks for session_authorization: check_session_authorization and assign_session_authorization
- The function is intentionally minimal, delegating all validation to the check hook
- Uses the role_auth_extra structure to receive pre-validated role information
- The newval parameter is ignored since the actual role information is passed through the extra parameter
- The function assumes that SetSessionAuthorization will succeed since validation was already performed
- Located in src/backend/commands/variable.c alongside the corresponding check hook
- Works in conjunction with the broader PostgreSQL role and authentication system
- The role_auth_extra structure is shared between session_authorization and role parameter hooks