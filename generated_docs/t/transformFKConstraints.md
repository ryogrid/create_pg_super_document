# transformFKConstraints

## Location
src/backend/parser/parse_utilcmd.c: 2726 - 2796

## Overview
Handles FOREIGN KEY constraints by generating ALTER TABLE ADD CONSTRAINT commands and managing validation settings for both table creation and column addition scenarios.

## Definition


## Detailed Description
The  function processes FOREIGN KEY constraints in a deferred manner, converting them into ALTER TABLE ADD CONSTRAINT commands that execute after the primary table creation or column addition operation completes. This approach is necessary because foreign key constraints require both the referencing and referenced tables to exist before validation can occur.

The function operates in two phases:
1. **Validation Handling**: When skipValidation is true (typically for CREATE TABLE or ADD COLUMN with NULL default), it marks constraints to skip validation while setting them as initially valid, overriding user-supplied NOT VALID flags
2. **Command Generation**: For scenarios other than explicit ADD CONSTRAINT operations, it creates AlterTableStmt nodes containing AlterTableCmd nodes for each foreign key constraint

The deferred execution ensures that foreign key constraints are added after all indexes are created and the table structure is complete, maintaining proper dependency ordering in the command execution sequence.

## Parameters / Member Variables
- : Pointer to CreateStmtContext containing the foreign key constraints list and execution context
- : Boolean indicating whether FK validation can be safely skipped (true for CREATE TABLE, ADD COLUMN with NULL default)
- : Boolean indicating whether this is called from an explicit ADD CONSTRAINT operation (affects command generation behavior)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new node structures)
  - lappend (appends to lists)
  - CreateStmtContext, Constraint, AlterTableStmt, AlterTableCmd (data structures)
  - AT_AddConstraint, OBJECT_TABLE (command type constants)
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md) (during CREATE TABLE processing)
  - [transformAlterTableStmt](transformAlterTableStmt.md) (during ALTER TABLE processing)

## Notes and Other Information
- This is a static function in parse_utilcmd.c, part of the constraint transformation infrastructure
- Implements deferred constraint creation to handle table dependency ordering
- Must execute after transformIndexConstraints to ensure proper command sequencing
- When isAddConstraint is true, no ALTER TABLE commands are generated (caller handles constraint addition)
- The skipValidation optimization applies to scenarios where no existing data needs validation
- Generated ALTER TABLE commands are added to cxt->alist for later execution
- Ensures foreign key constraints are processed consistently across CREATE TABLE and ALTER TABLE operations