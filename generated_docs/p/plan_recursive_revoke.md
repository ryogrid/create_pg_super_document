# plan_recursive_revoke

## Location
src/backend/commands/user.c: 2413 - 2502

## Overview
Recursively plans the revocation of role grants and their dependent privileges, handling complex cascading scenarios where grants depend on other grants through the grantor hierarchy.

## Definition
```c
static void plan_recursive_revoke(CatCList *memlist, RevokeRoleGrantAction *actions,
                                 int index, bool revoke_admin_option_only, 
                                 DropBehavior behavior)
```

## Detailed Description
This function is the core workhorse for handling recursive revocation of role grants, similar to how recursive_revoke() works for ACLs. It implements a sophisticated algorithm to handle dependent privileges when revoking grants:

1. **Early Exit Optimization**: Returns immediately if the action has already been planned for the target grant
2. **Admin Option Analysis**: Determines whether the member would still retain admin privileges from other grants after the revocation
3. **Dependency Checking**: Identifies all grants that depend on the current grant (where the member is the grantor)
4. **Cascade vs Restrict**: Enforces DROP_RESTRICT behavior by raising an error if dependent objects exist, or proceeds with CASCADE to automatically revoke dependents

The function ensures that privilege hierarchies remain consistent after revocation by recursively processing all affected grants.

## Parameters / Member Variables
- `memlist`: CatCList containing all role membership grants for the target role
- `actions`: Array of RevokeRoleGrantAction values tracking planned actions for each grant
- `index`: Index into the memlist identifying the specific grant being processed
- `revoke_admin_option_only`: Boolean indicating whether to revoke only the admin option or the entire grant
- `behavior`: DropBehavior specifying whether to use RESTRICT or CASCADE for dependent objects

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT
  - ereport
  - errcode
  - errmsg
  - errhint
  - plan_recursive_revoke (recursive call)
- Types used:
  - CatCList
  - RevokeRoleGrantAction
  - DropBehavior
  - Form_pg_auth_members
  - HeapTuple
- Constants:
  - RRG_DELETE_GRANT
  - RRG_REMOVE_ADMIN_OPTION
  - RRG_NOOP
  - DROP_RESTRICT
  - ERROR
  - ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST
- Called from:
  - plan_single_revoke
  - plan_member_revoke
  - plan_recursive_revoke (recursive)

## Notes and Other Information
- The function implements a depth-first traversal of the privilege dependency graph
- It checks if a member would still have admin options from other grants before proceeding with dependent revocations
- When behavior is DROP_RESTRICT, it raises a detailed error message suggesting the use of CASCADE
- The recursive nature ensures that all levels of privilege dependencies are properly handled
- Performance is optimized by early returns when actions have already been planned
- The function maintains consistency in complex role hierarchies where privileges can be granted through multiple paths
- Critical for maintaining referential integrity in PostgreSQL's role-based access control system