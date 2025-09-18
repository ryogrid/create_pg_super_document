# ATAddCheckConstraint

## Location
src/backend/commands/tablecmds.c: 9470 - 9606

## Overview
ATAddCheckConstraint adds a check constraint to a table and recursively applies it to all child tables in an inheritance hierarchy, ensuring consistent constraint naming across the hierarchy.

## Definition


## Detailed Description
This function implements check constraint addition with sophisticated inheritance handling. Unlike other ALTER TABLE operations that use prep-time recursion, this function performs execution-time recursion to ensure all constraints across the inheritance hierarchy receive the same name. This is critical because PostgreSQL requires related constraints to have identical names to be recognized as part of the same logical constraint.

The function uses AddRelationNewConstraints to create the actual constraint, handling constraint merging when appropriate (particularly for child tables that may already have compatible constraints). It manages a work queue system for deferred validation and carefully tracks whether constraints need validation through the NewConstraint structure.

For inheritance hierarchies, the function recursively descends one level at a time rather than using find_all_inheritors, allowing precise control over constraint propagation and name consistency. It includes safety checks for ONLY clauses and NO INHERIT constraints.

## Parameters / Member Variables
- : Double pointer to the work queue for managing ALTER TABLE operations across multiple tables
- : AlteredTableInfo structure for the current table being modified
- : Relation object representing the table receiving the constraint
- : Constraint specification including the check expression and properties
- : Boolean indicating whether to apply the constraint to child tables
- : Boolean indicating if this is a recursive call (affects permission checking)
- : Boolean indicating if this constraint is being re-added during a table rewrite
- : Lock mode to use when accessing child tables

## Dependencies
- Functions called/Symbols referenced:
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [AddRelationNewConstraints](AddRelationNewConstraints.md)
  - copyObject
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - CommandCounterIncrement
  - ObjectAddressSet
- Called from (representative examples):
  - [ATExecAddConstraint](ATExecAddConstraint.md)
  - [DetachAddConstraintIfNeeded](../D/DetachAddConstraintIfNeeded.md)
  - [ATAddCheckConstraint](ATAddCheckConstraint.md) (recursive calls)

## Notes and Other Information
- Performs execution-time rather than prep-time recursion to ensure consistent constraint naming
- Handles constraint merging for cases where child tables already have compatible constraints
- Includes sophisticated inheritance handling with proper lock management
- Supports NO INHERIT constraints that don't propagate to children
- Validates ONLY clause usage and prevents constraint addition when children exist but recursion is disabled
- Integrates with the work queue system for deferred constraint validation
- Uses CommandCounterIncrement to handle multiple visits to the same table
- Critical for maintaining constraint consistency across PostgreSQL inheritance hierarchies