# get_row_security_policies

## Location
[src/backend/rewrite/rowsecurity.c:98-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rowsecurity.c#L98-L540)

## Overview
This function retrieves and applies row-level security (RLS) policies for a specific relation in a query, enforcing access control through security qualifiers and with-check options.

## Definition

```c
void
get_row_security_policies(Query *root, RangeTblEntry *rte, int rt_index,
						  List **securityQuals, List **withCheckOptions,
						  bool *hasRowSecurity, bool *hasSubLinks)
```
## Detailed Description
The  function is the central coordinator for PostgreSQL's row-level security implementation. It analyzes a range table entry (RTE) and determines what security policies should be applied based on the command type, user permissions, and RLS configuration. The function handles complex scenarios including:

- Different command types (SELECT, INSERT, UPDATE, DELETE, MERGE)
- Mixed permission requirements (e.g., SELECT FOR UPDATE)
- INSERT...ON CONFLICT DO UPDATE scenarios
- MERGE command with multiple possible actions
- Permissive vs restrictive policy combinations

The function applies policies in a specific order to ensure proper privilege escalation - higher privileged operations (UPDATE/DELETE) are checked before lower privileged ones (SELECT).

## Parameters / Member Variables
- `*root`: The Query structure containing the entire query context
- `*rte`: The RangeTblEntry representing the relation being accessed
- `rt_index`: Index of this RTE in the query's range table
- `**securityQuals`: Output parameter - list of security qualifiers to enforce during row retrieval
- `**withCheckOptions`: Output parameter - list of with-check options to enforce during row modification
- `*hasRowSecurity`: Output parameter - set to true if RLS is enabled for this relation
- `*hasSubLinks`: Output parameter - set to true if any returned policies contain subqueries
## Dependencies
- Functions called/Symbols referenced:
  - [getRTEPermissionInfo](getRTEPermissionInfo.md)
  - [check_enable_rls](../c/check_enable_rls.md)
  - [get_policies_for_relation](get_policies_for_relation.md)
  - [add_security_quals](../a/add_security_quals.md)
  - [add_with_check_options](../a/add_with_check_options.md)
  - [table_open](../t/table_open.md)/table_close
  - [setRuleCheckAsUser](../s/setRuleCheckAsUser.md)
- Called from (representative examples):
  - [fireRIRrules](../f/fireRIRrules.md)

## Notes and Other Information
- The function handles the complex interaction between different command types and permission requirements
- For INSERT...ON CONFLICT DO UPDATE, it applies both INSERT and UPDATE policies appropriately
- For MERGE commands, it sets up policies for all possible actions (INSERT, UPDATE, DELETE)
- Security qualifiers are used for filtering existing rows, while with-check options validate new/modified rows
- The function respects the checkAsUser setting for privilege escalation scenarios
- Policy evaluation follows PostgreSQL's privilege hierarchy where UPDATE/DELETE policies are checked before SELECT policies

## Simplified Source

```c
void get_row_security_policies(Query *root, RangeTblEntry *rte, int rt_index,
                              List **securityQuals, List **withCheckOptions,
                              bool *hasRowSecurity, bool *hasSubLinks) {
    // Initialize return values
    *securityQuals = NIL;
    *withCheckOptions = NIL;
    *hasRowSecurity = false;
    *hasSubLinks = false;

    // Only process normal relations
    if (rte->relkind != RELKIND_RELATION &&
        rte->relkind != RELKIND_PARTITIONED_TABLE)
        return;

    // Determine effective user and RLS status
    perminfo = getRTEPermissionInfo(root->rteperminfos, rte);
    user_id = OidIsValid(perminfo->checkAsUser) ?
              perminfo->checkAsUser : GetUserId();
    rls_status = check_enable_rls(rte->relid, perminfo->checkAsUser, false);

    // Early exit if no RLS
    if (rls_status == RLS_NONE)
        return;
    if (rls_status == RLS_NONE_ENV) {
        *hasRowSecurity = true;
        return;
    }

    // Open relation and determine command type
    rel = table_open(rte->relid, NoLock);
    commandType = rt_index == root->resultRelation ?
                  root->commandType : CMD_SELECT;

    // Apply policies based on command type and permissions
    get_policies_for_relation(rel, commandType, user_id,
                             &permissive_policies, &restrictive_policies);

    // For SELECT/UPDATE/DELETE: add security qualifiers
    if (commandType == CMD_SELECT || commandType == CMD_UPDATE ||
        commandType == CMD_DELETE) {
        add_security_quals(rt_index, permissive_policies, restrictive_policies,
                          securityQuals, hasSubLinks);
    }

    // For INSERT/UPDATE: add with-check options
    if (commandType == CMD_INSERT || commandType == CMD_UPDATE) {
        add_with_check_options(rel, rt_index,
                              commandType == CMD_INSERT ?
                              WCO_RLS_INSERT_CHECK : WCO_RLS_UPDATE_CHECK,
                              permissive_policies, restrictive_policies,
                              withCheckOptions, hasSubLinks, false);
    }

    // Handle special cases for mixed permissions and complex commands
    // (SELECT FOR UPDATE, INSERT ON CONFLICT, MERGE operations)
    // Each case adds appropriate security qualifiers and with-check options

    table_close(rel, NoLock);

    // Propagate checkAsUser to subqueries
    setRuleCheckAsUser((Node *) *securityQuals, perminfo->checkAsUser);
    setRuleCheckAsUser((Node *) *withCheckOptions, perminfo->checkAsUser);

    *hasRowSecurity = true;
}
```