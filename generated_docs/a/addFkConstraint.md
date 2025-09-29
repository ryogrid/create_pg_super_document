# addFkConstraint

## Location
[src/backend/commands/tablecmds.c:10123-10295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L10123-L10295)

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
  - [ConstraintNameIsUsed](../C/ConstraintNameIsUsed.md)
  - [ChooseConstraintName](../C/ChooseConstraintName.md)
  - [ChooseForeignKeyConstraintNameAddition](../C/ChooseForeignKeyConstraintNameAddition.md)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - RelationGetNamespace
  - RelationGetRelid
  - RelationGetRelationName
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)
  - [addFkRecurseReferenced](addFkRecurseReferenced.md)
  - [addFkRecurseReferencing](addFkRecurseReferencing.md)
  - [CloneFkReferenced](../C/CloneFkReferenced.md)
  - [CloneFkReferencing](../C/CloneFkReferencing.md)

## Notes and Other Information
- This is a static function within tablecmds.c, part of the ALTER TABLE infrastructure
- The function does not create pg_trigger entries - that is handled separately by addFkRecurseReferenced and addFkRecurseReferencing
- For partitioned tables, special dependency relationships are created to ensure proper constraint management during partition operations
- [Constraint](../C/Constraint.md) names are automatically modified if conflicts exist, ensuring uniqueness within the relation
- The function handles both top-level constraints and partition-specific constraints differently regarding inheritance properties
- [Command](../C/Command.md) counter increment ensures constraint visibility for subsequent operations in the same transaction

## Simplified Source

```c
static ObjectAddress addFkConstraint(addFkConstraintSides fkside,
                                    char *constraintname, Constraint *fkconstraint,
                                    Relation rel, Relation pkrel, Oid indexOid, Oid parentConstr,
                                    int numfks, int16 *pkattnum, int16 *fkattnum,
                                    Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
                                    int numfkdelsetcols, int16 *fkdelsetcols, bool is_internal) {
    ObjectAddress address;
    Oid constraintOid;
    char *conname;
    bool conislocal, connoinherit;
    int coninhcount;

    // Verify referenced relation is a table
    if (pkrel->rd_rel->relkind != RELKIND_RELATION &&
        pkrel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
        ereport(ERROR, "referenced relation is not a table");
    }

    // Choose unique constraint name if needed
    if (ConstraintNameIsUsed(CONSTRAINT_RELATION, RelationGetRelid(rel), constraintname)) {
        conname = ChooseConstraintName(RelationGetRelationName(rel),
                                      ChooseForeignKeyConstraintNameAddition(fkconstraint->fk_attrs),
                                      "fkey", RelationGetNamespace(rel), NIL);
    } else {
        conname = constraintname;
    }

    // Set constraint name if not already set
    if (fkconstraint->conname == NULL)
        fkconstraint->conname = pstrdup(conname);

    // Set inheritance properties
    if (OidIsValid(parentConstr)) {
        // This is a child constraint
        conislocal = false;
        coninhcount = 1;
        connoinherit = false;
    } else {
        // This is a top-level constraint
        conislocal = true;
        coninhcount = 0;
        connoinherit = (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE);
    }

    // Create the constraint entry in pg_constraint
    constraintOid = CreateConstraintEntry(conname, RelationGetNamespace(rel),
                                         CONSTRAINT_FOREIGN, fkconstraint->deferrable,
                                         fkconstraint->initdeferred, fkconstraint->initially_valid,
                                         parentConstr, RelationGetRelid(rel), fkattnum, numfks,
                                         numfks, InvalidOid, indexOid, RelationGetRelid(pkrel),
                                         pkattnum, pfeqoperators, ppeqoperators, ffeqoperators,
                                         numfks, fkconstraint->fk_upd_action, fkconstraint->fk_del_action,
                                         fkdelsetcols, numfkdelsetcols, fkconstraint->fk_matchtype,
                                         NULL, NULL, NULL, conislocal, coninhcount, connoinherit,
                                         is_internal);

    ObjectAddressSet(address, ConstraintRelationId, constraintOid);

    // Set up dependency relationships for partitioned constraints
    if (OidIsValid(parentConstr)) {
        ObjectAddress referenced;
        ObjectAddressSet(referenced, ConstraintRelationId, parentConstr);

        if (fkside == addFkReferencedSide) {
            recordDependencyOn(&address, &referenced, DEPENDENCY_INTERNAL);
        } else {
            recordDependencyOn(&address, &referenced, DEPENDENCY_PARTITION_PRI);
            ObjectAddressSet(referenced, RelationRelationId, RelationGetRelid(rel));
            recordDependencyOn(&address, &referenced, DEPENDENCY_PARTITION_SEC);
        }
    }

    // Make constraint visible for subsequent operations
    CommandCounterIncrement();

    return address;
}
```