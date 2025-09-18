# ATExecAddConstraint

## Location
src/backend/commands/tablecmds.c: 9355 - 9427

## Overview
ATExecAddConstraint is the main dispatcher function for ALTER TABLE ADD CONSTRAINT operations, routing different constraint types to their specific implementation functions.

## Definition


## Detailed Description
This function serves as the central dispatch point for adding constraints to tables during ALTER TABLE operations. It examines the constraint type and delegates to the appropriate specialized function. Currently handles CHECK and FOREIGN KEY constraints, with the framework designed to easily accommodate additional constraint types in the future.

For CHECK constraints, it directly calls ATAddCheckConstraint. For FOREIGN KEY constraints, it performs additional name validation and generation before calling ATAddForeignKeyConstraint. If a foreign key constraint name is not provided, it automatically generates an appropriate name using ChooseConstraintName and ChooseForeignKeyConstraintNameAddition.

The function includes robust error handling for duplicate constraint names and unsupported constraint types, ensuring data integrity and providing clear error messages to users.

## Parameters / Member Variables
- : Double pointer to the work queue list for managing ALTER TABLE operations
- : AlteredTableInfo structure containing information about the table being altered
- : Relation object representing the target table
- : Constraint node specifying the constraint to be added
- : Boolean indicating whether to apply the constraint to inheritance children
- : Boolean indicating if this is a constraint being re-added (e.g., during table rewrite)
- : Lock mode to use during the operation

## Dependencies
- Functions called/Symbols referenced:
  - [ATAddCheckConstraint](ATAddCheckConstraint.md)
  - [ConstraintNameIsUsed](../C/ConstraintNameIsUsed.md)
  - [ChooseConstraintName](../C/ChooseConstraintName.md)
  - [ChooseForeignKeyConstraintNameAddition](../C/ChooseForeignKeyConstraintNameAddition.md)
  - [ATAddForeignKeyConstraint](ATAddForeignKeyConstraint.md)
  - RelationGetNamespace
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Currently supports only CHECK and FOREIGN KEY constraints, with extensible design for future constraint types
- Automatically generates foreign key constraint names when not specified by the user
- Validates constraint name uniqueness before creation to prevent conflicts
- Returns InvalidObjectAddress when no constraint is actually added
- Part of the larger ALTER TABLE infrastructure and integrates with the work queue system for complex multi-step operations
- Uses a switch statement design pattern to make adding new constraint types straightforward