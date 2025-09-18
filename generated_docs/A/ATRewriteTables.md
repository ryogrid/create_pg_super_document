# ATRewriteTables

## Location
src/backend/commands/tablecmds.c: 5702 - 5987

## Overview
ATRewriteTables is the third phase of ALTER TABLE processing that handles table rewrites, constraint validation, and final cleanup operations for all tables affected by the ALTER TABLE command.

## Definition


## Detailed Description
ATRewriteTables orchestrates the most resource-intensive phase of ALTER TABLE operations, handling physical table rewrites when necessary. The function processes each table in the work queue, determining whether a full table rewrite is required based on column type changes, persistence changes, or access method changes. For tables requiring rewrites, it creates a temporary table with the new structure, copies and transforms data through ATRewriteTable, then swaps the old and new table files.

The function implements sophisticated logic to handle various scenarios: full table rewrites for column type changes, constraint-only validation when no physical changes are needed, tablespace-only moves using block-by-block copying, and sequence persistence changes. It also manages composite type dependencies, ensuring that tables using a table's rowtype as a column type are properly updated when the source table structure changes.

The function operates in multiple distinct phases: first handling table rewrites and constraint validation, then processing foreign key constraints in a separate pass (since both sides of a foreign key relationship may have been rewritten), and finally executing any queued after-statements that were generated during the transformation phase.

## Parameters / Member Variables
- : Pointer to the original AlterTableStmt for event trigger reporting (NULL for internal operations)
- : Double pointer to the work queue list containing all AlteredTableInfo entries to process
- : Lock mode to acquire on relations during processing
- : Pointer to AlterTableUtilityContext for maintaining operation context

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_HAS_STORAGE
  - table_open
  - find_composite_type_dependencies
  - table_close
  - IsSystemRelation
  - RelationIsUsedAsCatalogTable
  - RELATION_IS_OTHER_TEMP
  - RelationGetRelationName
  - EventTriggerTableRewrite
  - make_new_heap
  - ATRewriteTable
  - finish_heap_swap
  - ReadNextMultiXactId
  - InvokeObjectPostAlterHook
  - SequenceChangePersistence
  - ATExecSetTableSpace
  - getOwnedSequences
  - validateForeignKeyConstraint
  - ProcessUtilityForAlterTable
  - CommandCounterIncrement
- Called from:
  - ATController

## Notes and Other Information
- This function is static and only used within the tablecmds.c module
- Implements multiple safety checks to prevent rewriting system catalogs, catalog tables, and temporary tables of other sessions
- Handles composite type dependencies by checking if other tables use the altered table's rowtype as a column type
- Creates temporary tables with appropriate persistence, tablespace, and access method settings before data copying
- Uses finish_heap_swap to atomically replace the old table with the new one after rewriting
- Processes foreign key constraints in a separate pass to ensure both sides of the relationship are fully rewritten before validation
- Manages sequence persistence changes to match their owning table's persistence
- Executes after-statements generated during the parse transformation phase
- Fires event triggers before table rewrites to allow extensions to hook into the process
- Located at src/backend/commands/tablecmds.c:5702-5987