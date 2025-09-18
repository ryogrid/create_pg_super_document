# SetOuterUserId

## Location
src/backend/utils/init/miscinit.c: 534 - 553

## Overview
SetOuterUserId sets the outer-level user ID and synchronizes both the effective user ID and superuser status to maintain consistent security context.

## Definition
static void SetOuterUserId(Oid userid, bool is_superuser)

## Detailed Description
SetOuterUserId is an internal static function that updates the OuterUserId variable along with related security state. When called, it:

1. Updates OuterUserId to the specified user ID
2. Forces CurrentUserId to match the new OuterUserId (maintaining consistency at the outer level)
3. Updates the is_superuser GUC parameter to reflect the superuser status of the new role

This function ensures that role changes maintain consistency across all user ID variables and related security settings. It is called internally by SET ROLE operations and session authorization changes.

## Parameters / Member Variables
- `userid`: Oid - The new outer user ID to set (must be valid)
- `is_superuser`: bool - Whether the new user ID has superuser privileges

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
  - OidIsValid (macro to validate the user ID)
  - SetConfigOption (function to update GUC parameters)
  - SecurityRestrictionContext (global security context variable)
  - OuterUserId (static variable to update)
  - CurrentUserId (static variable to synchronize)
- Called from (representative examples):
  - SetSessionAuthorization (when SET SESSION AUTHORIZATION is executed)
  - SetCurrentRoleId (when SET ROLE is executed)

## Notes and Other Information
- This is a static internal function, not exposed in the public API
- Includes assertions to ensure it is only called when SecurityRestrictionContext is 0 (no active security restrictions)
- The function maintains the invariant that at the outer level, CurrentUserId equals OuterUserId
- Updates the is_superuser GUC with PGC_INTERNAL context and PGC_S_DYNAMIC_DEFAULT source
- Critical for maintaining security context consistency during role switching operations
- The synchronization of CurrentUserId with OuterUserId is essential for proper permission checking at the outer transaction level