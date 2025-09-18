# addFkConstraint

## Location
src/backend/commands/tablecmds.c: 10123 - 10295

## Overview
Creates and installs pg_constraint entries to implement a foreign key constraint, handling constraint naming, inheritance properties, and dependency relationships for both regular and partitioned tables.

## Definition
```c
static ObjectAddress addFkConstraint(addFkConstraintSides fkside,
                                    char *constraintname, Constraint *fkconstraint,
                                    Relation rel, Relation pkrel, Oid indexOid, Oid parentConstr,
                                    int numfks, int16 *pkattnum,
                                    int16 *fkattnum, Oid *pfeqoperators, Oid *ppeqoperators,
                                    Oid *ffeqoperators, int numfkdelsetcols, int16 *fkdelsetcols,
                                    bool is_internal)
```

## Detailed Description
This function creates the core pg_constraint catalog entry for a foreign key constraint. It handles the complexity of constraint naming (generating unique names when conflicts exist), determines inheritance properties based on table types and parent constraints, and creates the appropriate dependency relationships for partitioned table scenarios.

The function verifies that the referenced relation is a valid table type, chooses an appropriate constraint name if needed, sets up inheritance properties based on whether this is a partitioned table or has a parent constraint, and creates dependency entries for proper constraint management in partitioned hierarchies.

Key responsibilities include:
- Validating referenced relation types
- Handling constraint name conflicts through automatic renaming
- Setting up proper inheritance flags for partitioned vs regular tables
- Creating constraint catalog entries with all necessary metadata
- Establishing dependency relationships for partitioned table constraints
- Managing constraint visibility through command counter increments

## Parameters / Member Variables
- `fkside`: Specifies which side of the FK relationship to create (referenced, referencing, or both)
- `constraintname`: Base name for the constraint (may be modified if conflicts exist)
- `fkconstraint`: The constraint definition structure containing FK properties
- `rel`: The root referencing relation (foreign key table)
- `pkrel`: The referenced relation (primary key table, may be a partition)
- `indexOid`: OID of the index implementing this constraint on the referenced table
- `parentConstr`: OID of parent constraint (InvalidOid for top-level constraints)
- `numfks`: Number of columns in the foreign key
- `pkattnum`: Array of attribute numbers for referenced columns
- `fkattnum`: Array of attribute numbers for referencing columns
- `pfeqoperators`: Array of equality operators between PK and FK columns
- `ppeqoperators`: Array of equality operators for PK columns
- `ffeqoperators`: Array of equality operators for FK columns
- `numfkdelsetcols`: Number of columns in ON DELETE SET NULL/DEFAULT clause
- `fkdelsetcols`: Array of attribute numbers for SET action columns
- `is_internal`: Whether this is an internal constraint

## Dependencies
- Functions called/Symbols referenced:
  - ConstraintNameIsUsed
  - ChooseConstraintName
  - ChooseForeignKeyConstraintNameAddition
  - CreateConstraintEntry
  - ObjectAddressSet
  - recordDependencyOn
  - CommandCounterIncrement
  - RelationGetNamespace
  - RelationGetRelid
  - RelationGetRelationName
- Called from (representative examples):
  - ATAddForeignKeyConstraint
  - addFkRecurseReferenced
  - addFkRecurseReferencing
  - CloneFkReferenced
  - CloneFkReferencing

## Notes and Other Information
- This is a static function within tablecmds.c, part of the ALTER TABLE infrastructure
- The function does not create pg_trigger entries - that is handled separately by addFkRecurseReferenced and addFkRecurseReferencing
- For partitioned tables, special dependency relationships are created to ensure proper constraint management during partition operations
- Constraint names are automatically modified if conflicts exist, ensuring uniqueness within the relation
- The function handles both top-level constraints and partition-specific constraints differently regarding inheritance properties
- Command counter increment ensures constraint visibility for subsequent operations in the same transaction