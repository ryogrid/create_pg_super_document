# ATExecAddOf

## Location
src/backend/commands/tablecmds.c: 16486 - 16627

## Overview
Executes the ALTER TABLE OF command to attach a table to a composite type, making it a typed table with structure matching the specified type.

## Definition


## Detailed Description
ATExecAddOf implements the ALTER TABLE OF SQL command that converts a regular table into a typed table by associating it with a composite type. The function performs comprehensive validation to ensure the table structure exactly matches the type definition, including column names, data types, type modifiers, and collations in the same order. It enforces that typed tables cannot have inheritance relationships and ensures the table structure is compatible with what could have been created using CREATE TABLE OF. If the table was previously typed, it removes the old type dependency before establishing the new one.

## Parameters / Member Variables
- : The relation to be converted to a typed table
- : TypeName structure identifying the composite type to attach to the table
- : Lock mode parameter for the operation

## Dependencies
- Functions called/Symbols referenced:
  - typenameType
  - check_of_type
  - table_open
  - systable_beginscan
  - systable_getnext
  - lookup_rowtype_tupdesc
  - TupleDescAttr
  - ReleaseTupleDesc
  - drop_parent_dependency
  - recordDependencyOn
  - SearchSysCacheCopy1
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - heap_freetuple
- Called from (representative examples):
  - ATExecCmd

## Notes and Other Information
- Validates that the table has no inheritance relationships, preventing typed tables from participating in inheritance
- Performs strict compatibility checking between table and type structure, requiring exact matches for column names, types, type modifiers, and collations
- Handles the case where a table is already typed by removing the previous type dependency before establishing the new one
- Updates pg_class.reloftype to record the type association
- Uses DEPENDENCY_NORMAL for the relationship between table and type
- Ensures that any extra columns beyond those in the type definition must be dropped columns
- Returns an ObjectAddress representing the composite type that was attached to the table
- Invokes post-alter hooks to notify other subsystems of the table modification