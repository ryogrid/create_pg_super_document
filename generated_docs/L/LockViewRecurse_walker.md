# LockViewRecurse_walker

## Location
src/backend/commands/lockcmds.c: 177 - 244

## Overview
A tree walker function that recursively traverses view query trees to lock all underlying base tables and nested views referenced by a view definition.

## Definition


## Detailed Description
LockViewRecurse_walker is a specialized tree walker that implements the deep locking semantics for views in PostgreSQL. When a view is locked, this function traverses its query tree structure to identify all referenced relations (tables and nested views) and applies the same lock mode to them. The function handles complex scenarios including self-referential views (preventing infinite recursion), permission checking with appropriate user context, and inheritance relationships. It uses PostgreSQL's query_tree_walker and expression_tree_walker infrastructure to systematically visit all nodes in the view's query definition.

## Parameters / Member Variables
- : Current node being examined in the query tree traversal
- : Context structure containing lock mode, user ID for permission checks, NOWAIT flag, and list of ancestor views to detect cycles

## Dependencies
- Functions called/Symbols referenced:
  - LockViewRecurse_context (structure type)
  - AclResult (enum type)
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