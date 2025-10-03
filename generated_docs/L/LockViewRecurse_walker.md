# LockViewRecurse_walker

## Location
[src/backend/commands/lockcmds.c:177-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/lockcmds.c#L177-L244)

## Overview
A tree walker function that recursively traverses view query trees to lock all underlying base tables and nested views referenced by a view definition.

## Definition

```c
static bool
LockViewRecurse_walker(Node *node, LockViewRecurse_context *context)
```
## Detailed Description
LockViewRecurse_walker is a specialized tree walker that implements the deep locking semantics for views in PostgreSQL. When a view is locked, this function traverses its query tree structure to identify all referenced relations (tables and nested views) and applies the same lock mode to them. The function handles complex scenarios including self-referential views (preventing infinite recursion), permission checking with appropriate user context, and inheritance relationships. It uses PostgreSQL's query_tree_walker and expression_tree_walker infrastructure to systematically visit all nodes in the view's query definition.

## Parameters / Member Variables
- `*node`: Current node being examined in the query tree traversal
- `*context`: Context structure containing lock mode, user ID for permission checks, NOWAIT flag, and list of ancestor views to detect cycles
## Dependencies
- Functions called/Symbols referenced:
  - LockViewRecurse_context (structure type)
  - [AclResult](../A/AclResult.md) (enum type)
  - [get_rel_name](../g/get_rel_name.md)
  - RELKIND_RELATION
  - RELKIND_PARTITIONED_TABLE
  - RELKIND_VIEW
  - [list_member_oid](../l/list_member_oid.md)
  - [LockTableAclCheck](LockTableAclCheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [LockRelationOid](LockRelationOid.md)
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md)
  - [LockViewRecurse](LockViewRecurse.md)
  - [LockTableRecurse](LockTableRecurse.md)
  - query_tree_walker
  - expression_tree_walker
  - [QTW_IGNORE_JOINALIASES](../Q/QTW_IGNORE_JOINALIASES.md)
- Called from (representative examples):
  - [LockViewRecurse](LockViewRecurse.md)
  - [LockViewRecurse_walker](LockViewRecurse_walker.md) (recursive calls)

## Notes and Other Information
- This is a static function, only accessible within the lockcmds.c module
- The function is designed to work with PostgreSQL's tree walker framework for systematic AST traversal
- Cycle detection prevents infinite recursion in self-referential views using the ancestor_views list
- Permission checking is performed with the appropriate user context (view owner or current user)
- The function handles both tables and views recursively, with different logic for inheritance vs view expansion
- [QTW_IGNORE_JOINALIASES](../Q/QTW_IGNORE_JOINALIASES.md) flag is used to avoid locking alias relations that don't represent real tables
- The walker pattern allows for extensible traversal of complex query structures while maintaining consistent locking semantics

## Simplified Source

```c
static bool LockViewRecurse_walker(Node *node, LockViewRecurse_context *context) {
    if (node == NULL)
        return false;

    if (IsA(node, Query)) {
        Query *query = (Query *) node;

        foreach(rtable, query->rtable) {
            RangeTblEntry *rte = lfirst(rtable);

            // Only process tables and views
            if (rte->relkind != RELKIND_RELATION &&
                rte->relkind != RELKIND_PARTITIONED_TABLE &&
                rte->relkind != RELKIND_VIEW)
                continue;

            // Skip self-referential views to avoid infinite recursion
            if (list_member_oid(context->ancestor_views, rte->relid))
                continue;

            // Check permissions for the lock
            AclResult aclresult = LockTableAclCheck(rte->relid, context->lockmode,
                                                  context->check_as_user);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, get_relkind_objtype(rte->relkind),
                             get_rel_name(rte->relid));

            // Acquire the lock
            if (!context->nowait)
                LockRelationOid(rte->relid, context->lockmode);
            else if (!ConditionalLockRelationOid(rte->relid, context->lockmode))
                ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                               errmsg("could not obtain lock on relation")));

            // Recursively process views and inherited tables
            if (rte->relkind == RELKIND_VIEW)
                LockViewRecurse(rte->relid, context->lockmode, context->nowait,
                              context->ancestor_views);
            else if (rte->inh)
                LockTableRecurse(rte->relid, context->lockmode, context->nowait);
        }

        return query_tree_walker(query, LockViewRecurse_walker, context,
                               QTW_IGNORE_JOINALIASES);
    }

    return expression_tree_walker(node, LockViewRecurse_walker, context);
}
```