# get_view_query

## Location
src/backend/rewrite/rewriteHandler.c: 2472 - 2510

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
  - list_length
  - linitial
  - elog
  - RewriteRule (struct)
  - RELKIND_VIEW (constant)
  - CMD_SELECT (constant)
- Called from (representative examples):
  - LockViewRecurse (lockcmds.c)
  - ATExecSetRelOptions (tablecmds.c)
  - rewriteTargetView (rewriteHandler.c)

## Notes and Other Information
- Caller must verify the relation is a view before calling this function
- Returns a read-only pointer into the relcache - must not be modified
- View _RETURN rules should always have exactly one action by design
- Part of PostgreSQL's view implementation using the rule system
- Essential for view expansion and query rewriting operations
- Used during view processing, locking operations, and DDL commands on views
- Provides the foundation for view transparency in PostgreSQL's query processing