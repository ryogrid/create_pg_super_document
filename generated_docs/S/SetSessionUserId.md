# SetSessionUserId

## Location
[src/backend/utils/init/miscinit.c:568-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L568-L580)

## Overview
Sets the session user ID and superuser status for the current PostgreSQL session, establishing the identity that will be used for permission checks and session-related operations.

## Definition

```c
static void
SetSessionUserId(Oid userid, bool is_superuser)
```
## Detailed Description
SetSessionUserId is an internal static function in PostgreSQL's authentication and session management system. It establishes the session user identity by setting the global SessionUserId variable and the corresponding superuser flag. This function is called during session initialization and authorization changes to update the session's user context.

The function includes assertions to ensure it's only called in appropriate contexts - specifically when no security restrictions are in place (SecurityRestrictionContext == 0) and with a valid user OID. The session user ID represents the authenticated user's identity for the duration of the session, which may differ from the current effective user ID if SET ROLE has been used.

## Parameters / Member Variables
- : The OID of the user to set as the session user. Must be a valid OID (not InvalidOid).
- : Boolean flag indicating whether the specified user has superuser privileges.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for validation checks)
  - OidIsValid (to validate the userid parameter)
- Called from (representative examples):
  - [SetSessionAuthorization](SetSessionAuthorization.md) (in miscinit.c:968)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the miscinit.c file
- The function updates two global variables: SessionUserId and SessionUserIsSuperuser
- Security assertions ensure the function is only called when SecurityRestrictionContext == 0
- The session user ID is distinct from the authenticated user ID and current user ID in PostgreSQL's role system
- This function is part of PostgreSQL's multi-layered user identity system that supports role switching via SET ROLE

## Simplified Source

```c
// Simplified version of SetSessionUserId
static void
SetSessionUserId(Oid userid, bool is_superuser)
{
    // Validate that we're in an unrestricted security context
    Assert(SecurityRestrictionContext == 0);

    // Ensure the user ID is valid
    Assert(OidIsValid(userid));

    // Set the session user identity
    SessionUserId = userid;
    SessionUserIsSuperuser = is_superuser;
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Preserved all original logic as the function is already quite simple
- Enhanced readability with clear variable purpose descriptions