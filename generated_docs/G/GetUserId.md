# GetUserId

## Location
[src/backend/utils/init/miscinit.c:515-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L515-L525)

## Overview
GetUserId returns the current effective user ID for permissions checking and security operations in PostgreSQL.

## Definition
Oid GetUserId(void)

## Detailed Description
GetUserId retrieves the current effective user ID stored in the CurrentUserId static variable. This is the primary function used throughout PostgreSQL for all normal permissions-checking purposes. The effective user ID can differ from the session user ID or outer user ID in cases where:

- SECURITY DEFINER functions are being executed
- Local user context changes are in effect (via SetUserIdAndSecContext)
- Security-restricted operations are active

The function includes an assertion to ensure that CurrentUserId contains a valid OID before returning it, helping to catch programming errors where the user context has not been properly initialized.

## Parameters / Member Variables
This function takes no parameters and returns:
- Return value: Oid - The current effective user ID (CurrentUserId)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
  - OidIsValid (macro to check if OID is valid)
  - CurrentUserId (static variable holding current effective user ID)
- Called from (representative examples):
  - [object_ownercheck](../o/object_ownercheck.md) functions for permission validation
  - Various catalog operations for ownership checks
  - PL/Tcl for user context in trusted procedures
  - Extension modules for access control

## Notes and Other Information
- There is no corresponding SetUserId() function; instead use SetUserIdAndSecContext() for changing the current user ID
- The function name is somewhat misleading as it returns the current effective user ID rather than the session user ID
- CurrentUserId can temporarily differ from OuterUserId during SECURITY DEFINER function calls
- The assertion makes this function unsuitable for use in error recovery paths; use GetUserIdAndSecContext() instead for such cases
- This is one of the core security functions in PostgreSQL privilege system

## Simplified Source

```c
// Simplified version of GetUserId
Oid GetUserId(void) {
    // Ensure the current user ID is valid before returning it
    Assert(OidIsValid(CurrentUserId));

    // Return the current effective user ID
    return CurrentUserId;
}
```

Key simplifications made:
- Added clear comments explaining the validation and return purpose
- Preserved the critical assertion for debugging and safety
- Focused on the core function: validate and return the current effective user ID
- Emphasized that this returns effective (not session) user ID