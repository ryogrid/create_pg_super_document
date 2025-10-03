# LockViewRecurse

## Location
[src/backend/commands/lockcmds.c:245-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/lockcmds.c#L245-L279)

## Overview
Recursively locks all underlying tables and nested views referenced by a given view, respecting security invoker semantics and preventing infinite recursion.

## Definition

```c
static void
LockViewRecurse(Oid reloid, LOCKMODE lockmode, bool nowait,
				List *ancestor_views)
```
## Detailed Description
LockViewRecurse implements the comprehensive locking mechanism for views in PostgreSQL's LOCK TABLE command. When a view is locked, it must also lock all the underlying relations (tables and nested views) that the view depends on to maintain consistency. This function sets up the necessary context for traversing the view's query definition and delegates the actual traversal to LockViewRecurse_walker. It handles security invoker views by adjusting the permission checking context appropriately - security invoker views check permissions as the current user, while standard views check permissions as the view owner. The function maintains an ancestor view list to detect and prevent infinite recursion in self-referential view definitions.

## Parameters / Member Variables
- `reloid`: OID of the view relation to recursively lock
- `lockmode`: Lock mode to apply to all underlying relations
- `nowait`: Boolean flag for conditional (non-blocking) lock acquisition
- `*ancestor_views`: List of ancestor view OIDs to detect recursive view references
## Dependencies
- Functions called/Symbols referenced:
  - LockViewRecurse_context (structure type)
  - [get_view_query](../g/get_view_query.md)
  - RelationHasSecurityInvoker
  - [lappend_oid](../l/lappend_oid.md)
  - [LockViewRecurse_walker](LockViewRecurse_walker.md)
  - [list_delete_last](../l/list_delete_last.md)
- Called from (representative examples):
  - [LockTableCommand](LockTableCommand.md)
  - [LockViewRecurse_walker](LockViewRecurse_walker.md)

## Notes and Other Information
- This is a static function, only accessible within the lockcmds.c module
- The function assumes the caller has already acquired a lock on the view itself
- Security invoker semantics are properly handled by adjusting the permission check context
- The ancestor_views list is managed carefully - added before walker call and removed after to maintain correct state
- View query parsing and relation opening/closing are handled with appropriate lock levels (NoLock since view is already locked)
- The function delegates the actual tree traversal work to LockViewRecurse_walker for separation of concerns
- Proper cleanup of the ancestor_views list ensures the calling context remains unchanged

## Simplified Source
```c
static void LockViewRecurse(Oid reloid, LOCKMODE lockmode, bool nowait, List *ancestor_views) {
    LockViewRecurse_context context;
    Relation view;
    Query *viewquery;

    // Open the view (caller already has lock)
    view = table_open(reloid, NoLock);
    viewquery = get_view_query(view);

    // Set up context for walker function
    context.lockmode = lockmode;
    context.nowait = nowait;

    // Use appropriate user for permission checks based on security invoker property
    if (RelationHasSecurityInvoker(view))
        context.check_as_user = GetUserId();  // Current user
    else
        context.check_as_user = view->rd_rel->relowner;  // View owner

    context.viewoid = reloid;
    context.ancestor_views = lappend_oid(ancestor_views, reloid);

    // Walk the view query to lock underlying relations
    LockViewRecurse_walker((Node *) viewquery, &context);

    // Clean up ancestor list
    context.ancestor_views = list_delete_last(context.ancestor_views);

    table_close(view, NoLock);
}
```