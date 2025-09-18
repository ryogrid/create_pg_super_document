# SetSessionUserId

## Location
src/backend/utils/init/miscinit.c: 568 - 580

## Overview
Sets the session user ID and superuser status for the current PostgreSQL session, establishing the identity that will be used for permission checks and session-related operations.

## Definition


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
  - SetSessionAuthorization (in miscinit.c:968)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the miscinit.c file
- The function updates two global variables: SessionUserId and SessionUserIsSuperuser
- Security assertions ensure the function is only called when SecurityRestrictionContext == 0
- The session user ID is distinct from the authenticated user ID and current user ID in PostgreSQL's role system
- This function is part of PostgreSQL's multi-layered user identity system that supports role switching via SET ROLE