# setRuleCheckAsUser

## Location
[src/backend/rewrite/rewriteDefine.c:631-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L631-L636)

## Overview
setRuleCheckAsUser recursively traverses a query or expression tree to set the checkAsUser field in all RTEPermissionInfos to a specified user ID for permission checking purposes.

## Definition

```c
void
setRuleCheckAsUser(Node *node, Oid userid)
```
## Detailed Description
setRuleCheckAsUser modifies the permission checking context within query trees by setting the checkAsUser field in all RTEPermissionInfo structures to a specified user ID. This function is crucial for implementing PostgreSQL's rule system security model, where rule actions may need to be executed with the permissions of the rule owner rather than the current user. The function uses a tree walker pattern to recursively traverse the entire node tree, including subqueries, common table expressions (CTEs), and sublinks, ensuring that all permission checking contexts are consistently updated. This is particularly important for view rules and security-definer functions where privilege escalation is intentional and controlled.

## Parameters / Member Variables
- `*node`: The root Node of the query or expression tree to traverse (can be a Query, expression, or any other Node type)
- `userid`: The OID of the user whose permissions should be used for access checks in the modified tree
## Dependencies
- Functions called/Symbols referenced:
  - [setRuleCheckAsUser_walker](setRuleCheckAsUser_walker.md) (internal helper function)
- Referenced by RTEPermissionInfo structure modification through:
  - [setRuleCheckAsUser_Query](setRuleCheckAsUser_Query.md) (processes Query nodes)
  - expression_tree_walker (traverses expression trees)
  - query_tree_walker (traverses query trees)
- Called from (representative examples):
  - [get_row_security_policies](../g/get_row_security_policies.md)
  - [RelationBuildRuleLock](../R/RelationBuildRuleLock.md)

## Notes and Other Information
- This function is part of PostgreSQL's security infrastructure for rules and views
- Used primarily in the context of rule expansion and permission checking where the rule owner's privileges should be used
- The function handles complex query structures including subqueries, CTEs, and sublinks to ensure comprehensive permission context updates
- Critical for maintaining security boundaries in rule-based access control and view definitions
- Works in conjunction with the broader permission checking system to support security-definer semantics for database rules
- The traversal is performed through specialized walker functions that understand PostgreSQL's query tree structure

## Simplified Source

```c
void setRuleCheckAsUser(Node *node, Oid userid) {
    // Delegate to walker function to recursively update permissions
    (void) setRuleCheckAsUser_walker(node, &userid);
}
```