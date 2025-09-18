# SetUserIdAndSecContext

## Location
[src/backend/utils/init/miscinit.c:665-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L665-L675)

## Overview
Sets both the current effective user ID and the security restriction context flags simultaneously for PostgreSQL's user context management system.

## Definition


## Detailed Description
SetUserIdAndSecContext is a low-level function that directly updates two critical global variables: CurrentUserId (the effective user ID for database operations) and SecurityRestrictionContext (bit flags indicating security restrictions). This function is designed to be used primarily for saving and restoring user context during transaction operations.

The function bypasses normal validation checks and is specifically intended for internal PostgreSQL operations where the caller is responsible for ensuring the validity of the parameters. It's commonly used during transaction abort/commit operations and in security-sensitive contexts like SECURITY DEFINER functions.

## Parameters / Member Variables
- : The user OID to set as the current effective user ID
- : Bit flags indicating security restrictions and operational context

## Dependencies
- Functions called/Symbols referenced:
  - CurrentUserId (global variable)
  - SecurityRestrictionContext (global variable)
- Called from (representative examples):
  - [AbortTransaction](../A/AbortTransaction.md)
  - [AbortSubTransaction](../A/AbortSubTransaction.md)  
  - [fmgr_security_definer](../f/fmgr_security_definer.md)
  - [RestoreUserContext](../R/RestoreUserContext.md)
  - [SwitchToUntrustedUser](SwitchToUntrustedUser.md)

## Notes and Other Information
- This function performs no validation on the input parameters - it's designed to work even with invalid values during error recovery scenarios
- Used extensively in transaction management (abort/commit) operations
- Critical for implementing SECURITY DEFINER function execution contexts
- Part of PostgreSQL's user context switching mechanism for privilege escalation/de-escalation
- Should not throw errors as it's used during error recovery paths
- Works in conjunction with GetUserIdAndSecContext for save/restore operations