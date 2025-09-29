# get_policies_for_relation

## Location
[src/backend/rewrite/rowsecurity.c:541-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rowsecurity.c#L541-L664)

## Overview
This static function retrieves and categorizes row-level security policies (both permissive and restrictive) that apply to a specific relation based on the command type and user role.

## Definition

```c
static void
get_policies_for_relation(Relation relation, CmdType cmd, Oid user_id,
						  List **permissive_policies,
						  List **restrictive_policies)
```
## Detailed Description
The  function is responsible for collecting all applicable row-level security policies for a given relation and command type. It examines both built-in policies stored in the relation descriptor and extension-provided policies through hooks. The function separates policies into two categories:

- **Permissive policies**: Combined using OR logic - if any permissive policy allows access, the row is accessible
- **Restrictive policies**: Combined using AND logic - all restrictive policies must allow access for the row to be accessible

The function handles the special 'ALL' command type ('*') which applies to all operations, and ensures that restrictive policies are processed in a deterministic order by sorting them by name.

## Parameters / Member Variables
- : The Relation structure representing the table being accessed
- : The command type (SELECT, INSERT, UPDATE, DELETE, MERGE) for which policies are being retrieved
- : The OID of the user/role for which to check policy applicability
- : Output parameter - list of applicable permissive policies
- : Output parameter - list of applicable restrictive policies

## Dependencies
- Functions called/Symbols referenced:
  - [check_role_for_policy](../c/check_role_for_policy.md)
  - [sort_policies_by_name](../s/sort_policies_by_name.md)
  - row_security_policy_hook_restrictive (hook)
  - row_security_policy_hook_permissive (hook)
- Called from (representative examples):
  - [get_row_security_policies](get_row_security_policies.md) (multiple times for different command types)

## Notes and Other Information
- The MERGE command type is handled specially - it doesn't have its own policies but derives them from other command types
- Restrictive policies from both built-in and hook sources are sorted by name to ensure deterministic ordering
- Extension hooks allow third-party code to provide additional policies beyond those stored in the system catalogs
- Built-in restrictive policies are always processed before hook-provided restrictive policies
- The function respects the policy's role list and only includes policies where the specified user has the appropriate role membership

## Simplified Source

```c
static void get_policies_for_relation(Relation relation, CmdType cmd, Oid user_id,
                                      List **permissive_policies,
                                      List **restrictive_policies) {
    *permissive_policies = NIL;
    *restrictive_policies = NIL;

    // Process built-in policies stored in relation descriptor
    foreach(item, relation->rd_rsdesc->policies) {
        RowSecurityPolicy *policy = (RowSecurityPolicy *) lfirst(item);
        bool cmd_matches = false;

        // Check if policy applies to this command type
        if (policy->polcmd == '*') {
            // ALL policies apply to all commands
            cmd_matches = true;
        } else {
            switch (cmd) {
                case CMD_SELECT:
                    cmd_matches = (policy->polcmd == ACL_SELECT_CHR);
                    break;
                case CMD_INSERT:
                    cmd_matches = (policy->polcmd == ACL_INSERT_CHR);
                    break;
                case CMD_UPDATE:
                    cmd_matches = (policy->polcmd == ACL_UPDATE_CHR);
                    break;
                case CMD_DELETE:
                    cmd_matches = (policy->polcmd == ACL_DELETE_CHR);
                    break;
                case CMD_MERGE:
                    // MERGE derives policies from other commands
                    break;
                default:
                    elog(ERROR, "unrecognized policy command type %d", (int) cmd);
            }
        }

        // Add policy if it applies to command and user role
        if (cmd_matches && check_role_for_policy(policy->roles, user_id)) {
            if (policy->permissive)
                *permissive_policies = lappend(*permissive_policies, policy);
            else
                *restrictive_policies = lappend(*restrictive_policies, policy);
        }
    }

    // Sort restrictive policies by name for deterministic order
    sort_policies_by_name(*restrictive_policies);

    // Add extension-provided restrictive policies
    if (row_security_policy_hook_restrictive) {
        List *hook_policies = (*row_security_policy_hook_restrictive)(cmd, relation);
        sort_policies_by_name(hook_policies);

        foreach(item, hook_policies) {
            RowSecurityPolicy *policy = (RowSecurityPolicy *) lfirst(item);
            if (check_role_for_policy(policy->roles, user_id))
                *restrictive_policies = lappend(*restrictive_policies, policy);
        }
    }

    // Add extension-provided permissive policies
    if (row_security_policy_hook_permissive) {
        List *hook_policies = (*row_security_policy_hook_permissive)(cmd, relation);

        foreach(item, hook_policies) {
            RowSecurityPolicy *policy = (RowSecurityPolicy *) lfirst(item);
            if (check_role_for_policy(policy->roles, user_id))
                *permissive_policies = lappend(*permissive_policies, policy);
        }
    }
}
```