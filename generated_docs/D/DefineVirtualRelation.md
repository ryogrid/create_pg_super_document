# DefineVirtualRelation

## Location
[src/backend/commands/view.c:45-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/view.c#L45-L266)

## Overview
DefineVirtualRelation creates a view relation and uses the rules system to store the query for the view, handling both new view creation and view replacement scenarios.

## Definition

```c
structure for this.
		 *
		 * Note that we must do this before updating the query for the view,
		 * since the rules system requires that the correct view columns be in
		 * place when defining the new rules.
		 *
		 * Also note that ALTER TABLE doesn't run parse transformation on
		 * AT_AddColumnToView commands.  The ColumnDef we supply must be ready
		 * to execute as-is.
		 */
		if (list_length(attrList) > rel->rd_att->natts)
		{
			ListCell   *c;
			int			skip = rel->rd_att->natts;

			foreach(c, attrList)
			{
				if (skip > 0)
				{
					skip--;
					continue;
				}
				atcmd = makeNode(AlterTableCmd);
				atcmd->subtype = AT_AddColumnToView;
				atcmd->def = (Node *) lfirst(c);
				atcmds = lappend(atcmds, atcmd);
			}

			/* EventTriggerAlterTableStart called by ProcessUtilitySlow */
			AlterTableInternal(viewOid, atcmds, true);

			/* Make the new view columns visible */
			CommandCounterIncrement();
		}

		/*
		 * Update the query for the view.
		 *
		 * Note that we must do this before updating the view options, because
		 * the new options may not be compatible with the old view query (for
		 * example if we attempt to add the WITH CHECK OPTION, we require that
		 * the new view be automatically updatable, but the old view may not
		 * have been).
		 */
		StoreViewQuery(viewOid, viewParse, replace);
```
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
  - [type_is_collatable](../t/type_is_collatable.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [relation_open](../r/relation_open.md), relation_close
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md)
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [checkViewColumns](../c/checkViewColumns.md)
  - [AlterTableInternal](../A/AlterTableInternal.md)
  - [StoreViewQuery](../S/StoreViewQuery.md)
  - [DefineRelation](DefineRelation.md)
  - ObjectAddressSet
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - CommandCounterIncrement

- Called from:
  - [DefineView](DefineView.md)

## Notes and Other Information
- This function must be called after EventTriggerAlterTableStart has been invoked
- The function handles collation resolution for view columns and reports errors for indeterminate collations
- For view replacement, it ensures that temporary views can only be replaced by temporary views, and permanent views by permanent views
- The function uses ALTER TABLE infrastructure for adding new columns during view replacement, which is noted as "overkill" but convenient
- Dependencies are carefully managed during replacement, with most view-level dependencies remaining unchanged while query dependencies are handled by StoreViewQuery