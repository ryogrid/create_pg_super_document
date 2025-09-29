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

## Simplified Source

```c
const char *
view_query_is_auto_updatable(Query *viewquery, bool check_cols)
{
    RangeTblRef *rtr;
    RangeTblEntry *base_rte;

    // Check SQL-92 auto-updatable view restrictions
    if (viewquery->distinctClause != NIL)
        return gettext_noop("Views containing DISTINCT are not automatically updatable.");

    if (viewquery->groupClause != NIL || viewquery->groupingSets)
        return gettext_noop("Views containing GROUP BY are not automatically updatable.");

    if (viewquery->havingQual != NULL)
        return gettext_noop("Views containing HAVING are not automatically updatable.");

    if (viewquery->setOperations != NULL)
        return gettext_noop("Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.");

    if (viewquery->cteList != NIL)
        return gettext_noop("Views containing WITH are not automatically updatable.");

    if (viewquery->limitOffset != NULL || viewquery->limitCount != NULL)
        return gettext_noop("Views containing LIMIT or OFFSET are not automatically updatable.");

    // Check for aggregates, window functions, and set-returning functions
    if (viewquery->hasAggs)
        return gettext_noop("Views that return aggregate functions are not automatically updatable.");

    if (viewquery->hasWindowFuncs)
        return gettext_noop("Views that return window functions are not automatically updatable.");

    if (viewquery->hasTargetSRFs)
        return gettext_noop("Views that return set-returning functions are not automatically updatable.");

    // Ensure exactly one base relation in FROM clause
    if (list_length(viewquery->jointree->fromlist) != 1)
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    rtr = (RangeTblRef *) linitial(viewquery->jointree->fromlist);
    if (!IsA(rtr, RangeTblRef))
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    // Validate base relation type
    base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);
    if (base_rte->rtekind != RTE_RELATION ||
        (base_rte->relkind != RELKIND_RELATION &&
         base_rte->relkind != RELKIND_FOREIGN_TABLE &&
         base_rte->relkind != RELKIND_VIEW &&
         base_rte->relkind != RELKIND_PARTITIONED_TABLE))
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    if (base_rte->tablesample)
        return gettext_noop("Views containing TABLESAMPLE are not automatically updatable.");

    // Check for at least one updatable column if required
    if (check_cols)
    {
        ListCell   *cell;
        bool        found = false;

        foreach(cell, viewquery->targetList)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(cell);

            if (view_col_is_auto_updatable(rtr, tle) == NULL)
            {
                found = true;
                break;
            }
        }

        if (!found)
            return gettext_noop("Views that have no updatable columns are not automatically updatable.");
    }

    return NULL;  // View is auto-updatable
}
```