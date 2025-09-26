# view_query_is_auto_updatable

## Location
[src/backend/rewrite/rewriteHandler.c:2623-2770](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L2623-L2770)

## Overview
Tests whether a view definition represents an auto-updatable view according to SQL standards, returning NULL if updatable or an error message explaining why it cannot be automatically updated.

## Definition
const char *view_query_is_auto_updatable(Query *viewquery, bool check_cols)

## Detailed Description
This function implements the core logic for determining whether a view can be automatically updated by PostgreSQL without requiring INSTEAD OF triggers. It performs comprehensive validation against SQL-92 auto-updatable view standards, with some extensions and relaxations specific to PostgreSQL.

The function checks for various restrictions that would prevent automatic updatability, including DISTINCT clauses, GROUP BY/HAVING clauses, set operations, window functions, aggregates, and complex FROM clauses. It ensures that each row in the view corresponds to a unique row in exactly one underlying base relation.

When check_cols is true, the function also verifies that at least one column in the view is updatable, which is necessary for INSERT and UPDATE operations but not for DELETE operations.

## Parameters / Member Variables
- `viewquery`: Query structure representing the view definition to analyze for auto-updatability
- `check_cols`: Boolean flag indicating whether to verify that the view has at least one updatable column (required for INSERT/UPDATE operations)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeTblRef](../R/RangeTblRef.md) (structure for range table references)
  - rt_fetch (function to retrieve range table entries)
  - RTE_RELATION (constant for relation range table entry type)
  - RELKIND_RELATION, RELKIND_FOREIGN_TABLE, RELKIND_VIEW, RELKIND_PARTITIONED_TABLE (relation kind constants)
  - [view_col_is_auto_updatable](view_col_is_auto_updatable.md) (function to check individual column updatability)
  - gettext_noop (macro for marking translatable strings)
  - IsA (macro for type checking)
  - linitial (macro to get first list element)
  - [list_length](../l/list_length.md) (function to get list length)
- Called from (representative examples):
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md) (in src/backend/commands/tablecmds.c:15146)
  - [DefineView](../D/DefineView.md) (in src/backend/commands/view.c:435)
  - [rewriteTargetView](../r/rewriteTargetView.md) (in src/backend/rewrite/rewriteHandler.c:3272)

## Notes and Other Information
- Implements SQL-92 auto-updatable view standards with PostgreSQL-specific extensions and relaxations
- Does not recursively check the updatability of underlying base relations - that is handled elsewhere
- Relaxes the SQL-92 restriction against subqueries in WHERE clauses due to PostgreSQL MVCC semantics
- Supports mixed updatable and non-updatable columns per SQL:1999 feature T111
- Imposes additional restrictions beyond SQL-92: no CTEs, no LIMIT/OFFSET, no system columns, no window functions, no set-returning functions
- The check_cols parameter allows the function to be used for different types of operations (DELETE vs INSERT/UPDATE)
- Returns const char* error messages marked for internationalization but not translated by this function
- Only accepts views that select from exactly one base relation (table, foreign table, view, or partitioned table)