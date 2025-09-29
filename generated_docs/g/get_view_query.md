# get_view_query

## Location
[src/backend/rewrite/rewriteHandler.c:2472-2510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L2472-L2510)

## Overview
Retrieves the Query structure from a view's _RETURN rule, providing access to the view's underlying SELECT query definition.

## Definition
Query *get_view_query(Relation view)

## Detailed Description
get_view_query is a utility function that extracts the underlying SELECT query from a view's rule system. In PostgreSQL, views are implemented using the rule system where each view has a special _RETURN rule with a CMD_SELECT event that contains the view's definition query.

The function iterates through all rules attached to the view relation, looking specifically for a rule with a CMD_SELECT event. Once found, it validates that the rule has exactly one action (as required by PostgreSQL's view implementation) and returns the Query structure from that action.

The returned pointer is directly from the relation cache (relcache), meaning it must be treated as read-only and should not be modified by the caller. If modifications are needed, the caller must create a copy using copyObject() or similar functions.

## Parameters / Member Variables
- view: Relation structure representing the view (must have relkind == RELKIND_VIEW)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro)
  - [list_length](../l/list_length.md)
  - linitial
  - elog
  - [RewriteRule](../R/RewriteRule.md) (struct)
  - RELKIND_VIEW (constant)
  - CMD_SELECT (constant)
- Called from (representative examples):
  - [LockViewRecurse](../L/LockViewRecurse.md) (lockcmds.c)
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md) (tablecmds.c)
  - [rewriteTargetView](../r/rewriteTargetView.md) (rewriteHandler.c)

## Notes and Other Information
- Caller must verify the relation is a view before calling this function
- Returns a read-only pointer into the relcache - must not be modified
- View _RETURN rules should always have exactly one action by design
- Part of PostgreSQL's view implementation using the rule system
- Essential for view expansion and query rewriting operations
- Used during view processing, locking operations, and DDL commands on views
- Provides the foundation for view transparency in PostgreSQL's query processing

## Simplified Source

```c
Query *get_view_query(Relation view) {
    // Assert that we have a view relation
    Assert(view->rd_rel->relkind == RELKIND_VIEW);

    // Search through all rules for the SELECT rule
    for (int i = 0; i < view->rd_rules->numLocks; i++) {
        RewriteRule *rule = view->rd_rules->rules[i];

        if (rule->event == CMD_SELECT) {
            // Validate _RETURN rule has exactly one action
            if (list_length(rule->actions) != 1)
                elog(ERROR, "invalid _RETURN rule action specification");

            // Return the Query from the first (and only) action
            return (Query *) linitial(rule->actions);
        }
    }

    // Should never reach here for a proper view
    elog(ERROR, "failed to find _RETURN rule for view");
    return NULL;
}
```