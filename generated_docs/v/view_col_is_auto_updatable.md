# view_col_is_auto_updatable

## Location
src/backend/rewrite/rewriteHandler.c: 2575 - 2622

## Overview
Tests whether a specific column of a view is auto-updatable, returning NULL if updatable or an error message string explaining why it cannot be updated.

## Definition
static const char *view_col_is_auto_updatable(RangeTblRef *rtr, TargetEntry *tle)

## Detailed Description
This function performs local checks on a single view column to determine if it can be automatically updated by PostgreSQL auto-updatable view mechanism. It validates that the column meets the basic requirements for auto-updatability: it must be a simple variable reference to a user column of the underlying base relation, not a computed expression, system column, or junk column.

The function is part of the view updatability analysis system and focuses solely on the view-level constraints. It does not verify whether the referenced column in the underlying base relation is itself updatable - that check is performed elsewhere in the system.

## Parameters / Member Variables
- `rtr`: RangeTblRef representing the range table entry for the view being analyzed
- `tle`: TargetEntry representing the specific column/expression in the view definition to check for updatability

## Dependencies
- Functions called/Symbols referenced:
  - RangeTblRef (structure representing range table references)
  - [TargetEntry](../T/TargetEntry.md) (structure representing target list entries)
  - Var (node type for variable references)
  - IsA (macro for type checking)
  - gettext_noop (macro for marking translatable strings)
- Called from (representative examples):
  - [view_query_is_auto_updatable](view_query_is_auto_updatable.md) (in src/backend/rewrite/rewriteHandler.c:2732)
  - [view_cols_are_auto_updatable](view_cols_are_auto_updatable.md) (in src/backend/rewrite/rewriteHandler.c:2801)

## Notes and Other Information
- Returns NULL for updatable columns, or a const char* error message for non-updatable columns
- Error messages are marked with gettext_noop for internationalization but are not translated by this function
- Only simple Var nodes referring to user columns (varattno > 0) of the base relation are considered updatable
- Resjunk columns (used internally by PostgreSQL) are explicitly rejected as non-updatable
- System columns (varattno < 0) and whole-row references (varattno = 0) are not allowed in updatable views
- The function enforces that the variable must refer to the correct range table entry (varno = rtr->rtindex) and the current query level (varlevelsup = 0)