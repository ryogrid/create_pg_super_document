# DefineVirtualRelation

## Location
src/backend/commands/view.c: 45 - 266

## Overview
DefineVirtualRelation creates a view relation and uses the rules system to store the query for the view, handling both new view creation and view replacement scenarios.

## Definition


## Detailed Description
DefineVirtualRelation is an internal function that handles the core mechanics of view creation in PostgreSQL. It performs two primary operations depending on whether it's creating a new view or replacing an existing one:

1. **New View Creation**: Creates a new relation with RELKIND_VIEW, constructs column definitions from the target list, and stores the view query using the rules system.

2. **View Replacement**: When the  flag is true and a view with the same name exists, it performs a sophisticated replacement process that includes:
   - Validating that the existing object is actually a view
   - Checking column compatibility between old and new views
   - Adding new columns if the new view has more columns than the old one
   - Updating the view query and options while preserving existing dependencies

The function constructs ColumnDef nodes from the target list entries, handling type information, collation, and ensuring non-junk columns are processed correctly. It also manages locking, namespace resolution, and dependency tracking.

## Parameters / Member Variables
- : RangeVar specifying the view name and namespace information
- : List of TargetEntry nodes representing the columns in the view's SELECT list
- : Boolean flag indicating whether this is a CREATE OR REPLACE VIEW operation
- : List of view options (e.g., WITH CHECK OPTION)
- : Query tree representing the view's SELECT statement

## Dependencies
- Functions called/Symbols referenced:
  - makeColumnDef
  - exprType, exprTypmod, exprCollation
  - type_is_collatable
  - RangeVarGetAndCheckCreationNamespace
  - relation_open, relation_close
  - CheckTableNotInUse
  - BuildDescForRelation
  - checkViewColumns
  - AlterTableInternal
  - StoreViewQuery
  - DefineRelation
  - ObjectAddressSet
  - recordDependencyOnCurrentExtension
  - CommandCounterIncrement

- Called from:
  - DefineView

## Notes and Other Information
- This function must be called after EventTriggerAlterTableStart has been invoked
- The function handles collation resolution for view columns and reports errors for indeterminate collations
- For view replacement, it ensures that temporary views can only be replaced by temporary views, and permanent views by permanent views
- The function uses ALTER TABLE infrastructure for adding new columns during view replacement, which is noted as "overkill" but convenient
- Dependencies are carefully managed during replacement, with most view-level dependencies remaining unchanged while query dependencies are handled by StoreViewQuery