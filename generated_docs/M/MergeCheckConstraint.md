# MergeCheckConstraint

## Location
[src/backend/commands/tablecmds.c:3052-3117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3052-L3117)

## Overview
A utility function that manages the merging of CHECK constraints during table inheritance, ensuring constraint consistency and proper inheritance counting.

## Definition
static List *MergeCheckConstraint(List *constraints, const char *name, Node *expr)

## Detailed Description
This function handles the complex logic of merging CHECK constraints when inheriting from multiple parent tables. PostgreSQL allows tables to inherit constraints from their parents, but when multiple parents define constraints with the same name, they must be carefully merged or validated for conflicts.

The function implements three distinct behaviors:
1. **Constraint Merging**: If an identically-named constraint with the same expression already exists, it increments the inheritance count to track how many parents contribute this constraint
2. **Conflict Detection**: If a constraint with the same name but different expression is found, it throws an error since conflicting constraints cannot be resolved automatically  
3. **New Constraint Addition**: If no matching constraint name is found, it creates a new CookedConstraint structure and adds it to the constraint list

The inheritance count tracking is crucial for PostgreSQL's constraint management system, as it determines how constraints behave during ALTER TABLE operations and prevents constraints that are inherited from multiple sources from being accidentally dropped.

## Parameters / Member Variables
- constraints: List of existing CookedConstraint structures representing previously processed constraints
- name: String name of the CHECK constraint being merged
- expr: Node representing the constraint expression to be merged

## Dependencies
- Functions called/Symbols referenced:
  - CookedConstraint (constraint structure type)
  - CONSTR_CHECK (constraint type enumeration)
  - [equal](../e/equal.md) (expression comparison function)
  - palloc0_object (memory allocation macro)
  - [pstrdup](../p/pstrdup.md) (string duplication function)
  - lappend (list append function)
  - ereport (error reporting function)
- Called from (representative examples):
  - [MergeAttributes](MergeAttributes.md) (main attribute merging function during inheritance)

## Notes and Other Information
- This is a static function within tablecmds.c, used specifically during table inheritance processing
- The function maintains inheritance counts to track constraint dependencies across multiple parent tables
- Prevents inheritance count overflow by checking for negative values (indicating wraparound)
- Uses deep comparison of constraint expressions via the equal() function to detect true duplicates vs naming conflicts
- Critical for maintaining constraint consistency in PostgreSQL's inheritance hierarchy
- The returned list may be the same as the input (if merging occurred) or a new list (if a constraint was added)