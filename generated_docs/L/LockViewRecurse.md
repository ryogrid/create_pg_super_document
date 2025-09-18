# LockViewRecurse

## Location
src/backend/commands/lockcmds.c: 245 - 279

## Overview
Recursively locks all underlying tables and nested views referenced by a given view, respecting security invoker semantics and preventing infinite recursion.

## Definition


## Detailed Description
LockViewRecurse implements the comprehensive locking mechanism for views in PostgreSQL's LOCK TABLE command. When a view is locked, it must also lock all the underlying relations (tables and nested views) that the view depends on to maintain consistency. This function sets up the necessary context for traversing the view's query definition and delegates the actual traversal to LockViewRecurse_walker. It handles security invoker views by adjusting the permission checking context appropriately - security invoker views check permissions as the current user, while standard views check permissions as the view owner. The function maintains an ancestor view list to detect and prevent infinite recursion in self-referential view definitions.

## Parameters / Member Variables
- : OID of the view relation to recursively lock
- : Lock mode to apply to all underlying relations
- : Boolean flag for conditional (non-blocking) lock acquisition
- : List of ancestor view OIDs to detect recursive view references

## Dependencies
- Functions called/Symbols referenced:
  - LockViewRecurse_context (structure type)
  - get_view_query
  - RelationHasSecurityInvoker
  - lappend_oid
  - LockViewRecurse_walker
  - list_delete_last
- Called from (representative examples):
  - LockTableCommand
  - LockViewRecurse_walker

## Notes and Other Information
- This is a static function, only accessible within the lockcmds.c module
- The function assumes the caller has already acquired a lock on the view itself
- Security invoker semantics are properly handled by adjusting the permission check context
- The ancestor_views list is managed carefully - added before walker call and removed after to maintain correct state
- View query parsing and relation opening/closing are handled with appropriate lock levels (NoLock since view is already locked)
- The function delegates the actual tree traversal work to LockViewRecurse_walker for separation of concerns
- Proper cleanup of the ancestor_views list ensures the calling context remains unchanged