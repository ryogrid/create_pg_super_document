# is_member_of_role_nosuper

## Location
src/backend/utils/adt/acl.c: 5259 - 5280

## Overview
Determines whether a given user/role is a member of another role, either directly or indirectly through role inheritance chains, but explicitly ignores superuser privileges.

## Definition


## Detailed Description
This function is identical to `is_member_of_role` except it does not grant automatic membership to superusers. It checks if a member (user or role) is a member of a target role through PostgreSQL's role membership system via recursive traversal of the role inheritance hierarchy, following both inherited and non-inherited grants.

The key difference from `is_member_of_role` is that superusers are not automatically considered members of every role - only explicit role memberships are considered.

**Important Usage Warning**: Like its counterpart, this function should not be used for privilege checking - use `has_privs_of_role()` instead.

## Parameters / Member Variables
- `member`: The OID of the user/role being tested for membership
- `role`: The OID of the target role to check membership against

## Dependencies
- Functions called/Symbols referenced:
  - `list_member_oid`: Searches for the target role in a list of roles
  - `roles_is_member_of`: Recursively finds all roles that member belongs to
  - `ROLERECURSE_MEMBERS`: Constant controlling recursion behavior
- Called from (representative examples):
  - `AddRoleMems`: Role membership management in user commands
  - `is_member`: HBA (Host-Based Authentication) membership checking
  - Various ACL-related functions for role validation

## Notes and Other Information
- Returns `true` immediately if member equals role (identity check)
- Unlike `is_member_of_role`, superusers are NOT automatically considered members of every role
- The function recursively traverses the entire role membership hierarchy
- Primarily used in contexts where superuser privilege escalation should not apply
- This variant is more appropriate for authentication and role management scenarios where explicit membership is required
- Located in `src/backend/utils/adt/acl.c:5259-5280`