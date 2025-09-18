# DefineViewRules

## Location
[src/backend/commands/view.c:332-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/view.c#L332-L355)

## Overview
DefineViewRules creates the rewrite rules for a view, specifically the ON SELECT rule that defines how SELECT queries against the view should be rewritten to use the underlying query.

## Definition


## Detailed Description
DefineViewRules is responsible for creating the rule system infrastructure that makes views functional in PostgreSQL. Currently, it creates only the ON SELECT rule, which is the fundamental rule that defines how SELECT statements against the view are rewritten to execute the view's underlying query.

The function uses DefineQueryRewrite() to create the rule, passing:
- The standard view select rule name (ViewSelectRuleName)
- The view's OID as the target relation
- CMD_SELECT as the event type
- The parsed query tree representing the view's SELECT statement

The function includes a comment indicating that automatic ON INSERT, ON UPDATE, and ON DELETE rules may be supported in the future, but currently only handles the SELECT case. For updatable views, separate rules would need to be created through other mechanisms.

Since the query has already undergone parse analysis, the function can directly use DefineQueryRewrite() without additional parsing steps.

## Parameters / Member Variables
- : Object identifier of the view relation for which rules are being created
- : Query tree representing the view's SELECT statement, already parsed and analyzed
- : Boolean indicating whether existing rules should be replaced (for CREATE OR REPLACE VIEW)

## Dependencies
- Functions called/Symbols referenced:
  - [DefineQueryRewrite](DefineQueryRewrite.md)
  - [pstrdup](../p/pstrdup.md)
  - list_make1
  - ViewSelectRuleName (constant/macro)
  - CMD_SELECT (constant)

- Called from:
  - [StoreViewQuery](../S/StoreViewQuery.md)

## Notes and Other Information
- Currently only implements ON SELECT rules; automatic INSERT/UPDATE/DELETE rules are planned for future implementation
- The function assumes the query has already been through complete parse analysis
- Uses the standard PostgreSQL rule system infrastructure via DefineQueryRewrite()
- The ViewSelectRuleName is a predefined constant that provides a standard naming convention for view select rules
- This is a relatively simple wrapper around DefineQueryRewrite() that handles the view-specific aspects of rule creation