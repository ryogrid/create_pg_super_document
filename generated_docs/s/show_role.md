# show_role

## Location
[src/backend/commands/variable.c:1022-1046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1022-L1046)

## Overview
The `show_role` function returns the current role name for display purposes, handling special cases where no role is set or when SET SESSION AUTHORIZATION affects role visibility.

## Definition
```c
const char *show_role(void)
```

## Detailed Description
This function is a GUC (Grand Unified Configuration) show hook that provides the string representation of the current role for SHOW ROLE commands and similar queries. It implements important logic to handle the interaction between SET ROLE and SET SESSION AUTHORIZATION commands.

The function addresses a specific PostgreSQL behavior where SET SESSION AUTHORIZATION logically resets SET ROLE to NONE, but the system cannot directly update the GUC role variable during the session authorization assignment. To handle this complexity, the function checks the actual current role ID rather than relying solely on the GUC string value.

Key behaviors:
1. **Role Validity Check**: Verifies if a role is currently active using the session management system
2. **Fallback Logic**: Returns "none" when no valid role is set, regardless of GUC variable state
3. **GUC Integration**: Uses the role_string variable when a valid role is active
4. **Null Safety**: Provides "none" fallback if role_string is unexpectedly null

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentRoleId](../G/GetCurrentRoleId.md)
  - role_string (external variable)
- Called from (representative examples):
  - GUC system framework (as show hook)

## Notes and Other Information
- Implements a workaround for SET SESSION AUTHORIZATION and SET ROLE interaction complexity
- Part of PostgreSQL's GUC hook mechanism for displaying configuration values
- Returns string literals that do not need to be freed by the caller
- Critical for providing accurate role information in SHOW commands and pg_settings views
- The "kluge" comment indicates this is a practical solution to architectural constraints
- Ensures consistent behavior between internal role tracking and user-visible role display