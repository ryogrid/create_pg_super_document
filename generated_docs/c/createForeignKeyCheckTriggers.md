# createForeignKeyCheckTriggers

## Location
src/backend/commands/tablecmds.c: 12536 - 12555

## Overview
Creates the referencing-side "check" triggers that implement foreign key constraint validation on the referencing table for INSERT and UPDATE operations.

## Definition


## Detailed Description
This function creates two check triggers on the referencing table that validate foreign key constraints when rows are inserted or updated. It acts as a wrapper around CreateFKCheckTrigger, calling it twice to create separate triggers for INSERT and UPDATE events. These triggers ensure that foreign key values in the referencing table correspond to valid primary key values in the referenced table.

## Parameters / Member Variables
- : OID of the referencing relation (foreign key table)
- : OID of the referenced relation (primary key table)
- : Constraint definition containing FK specification
- : OID of the foreign key constraint
- : OID of the index supporting the foreign key
- : OID of parent insert trigger (for inheritance)
- : OID of parent update trigger (for inheritance)
- : Output parameter for created insert check trigger OID
- : Output parameter for created update check trigger OID

## Dependencies
- Functions called/Symbols referenced:
  - [CreateFKCheckTrigger](../C/CreateFKCheckTrigger.md) (trigger creation for INSERT and UPDATE)
- Called from (representative examples):
  - child_dependency_type
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)

## Notes and Other Information
- Complementary to createForeignKeyActionTriggers which creates triggers on the referenced table
- Creates triggers that fire before INSERT/UPDATE to validate foreign key references
- Uses CreateFKCheckTrigger with different parameters for INSERT (true) vs UPDATE (false) operations
- Part of the complete foreign key constraint implementation in PostgreSQL