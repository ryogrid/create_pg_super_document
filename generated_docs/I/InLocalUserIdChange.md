# InLocalUserIdChange

## Location
src/backend/utils/init/miscinit.c: 676 - 684

## Overview
Checks whether PostgreSQL is currently executing within a local user ID change operation by examining security restriction context flags.

## Definition
```c
bool InLocalUserIdChange(void)
```

## Detailed Description
InLocalUserIdChange is a query function that determines if the current execution context is inside a temporary user ID change operation. It examines the SECURITY_LOCAL_USERID_CHANGE bit in the SecurityRestrictionContext global variable to make this determination.

This function is essential for understanding when the effective CurrentUserId has been temporarily modified and may not reflect the session's original user identity. It's used by PostgreSQL's security and configuration systems to determine appropriate behavior when user context switching is active.

## Parameters / Member Variables
None - this function takes no parameters and returns a boolean value.

## Dependencies
- Functions called/Symbols referenced:
  - SecurityRestrictionContext (global variable)
  - SECURITY_LOCAL_USERID_CHANGE (macro constant: 0x0001)
- Called from (representative examples):
  - GetUserIdAndContext
  - set_config_with_handle
  - AmSpecialWorkerProcess

## Notes and Other Information
- Returns true when SECURITY_LOCAL_USERID_CHANGE flag is set in SecurityRestrictionContext
- Used primarily for security checks and configuration validation
- Part of PostgreSQL's user context switching mechanism
- The flag is typically set during SECURITY DEFINER function execution or other privilege escalation scenarios
- Helps maintain security boundaries when user ID changes are temporary and localized