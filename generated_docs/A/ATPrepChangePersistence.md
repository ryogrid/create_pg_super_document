# ATPrepChangePersistence

## Location
[src/backend/commands/tablecmds.c:17088-17206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17088-L17206)

## Overview
ATPrepChangePersistence validates and prepares for changing a table's persistence level (LOGGED/UNLOGGED) by checking constraints and publication membership to maintain referential integrity invariants.

## Definition

```c
static bool
ATPrepChangePersistence(Relation rel, bool toLogged)
```
## Detailed Description
This function serves as the preparation phase for SET LOGGED/UNLOGGED operations in ALTER TABLE commands. It performs several critical validation checks to ensure the persistence change is valid and safe:

1. Prevents persistence changes on temporary tables (which is not allowed)
2. Returns early if the operation is a no-op (table is already in the target persistence state)
3. Validates publication membership constraints (unlogged tables cannot be part of publications)
4. Checks foreign key constraints to preserve the invariant that permanent tables cannot reference unlogged tables

The function examines both incoming and outgoing foreign key relationships and ensures that changing persistence will not violate PostgreSQL's referential integrity rules. Self-referencing foreign keys are safely ignored during this validation.

## Parameters / Member Variables
- `rel`: The Relation structure representing the table whose persistence is being changed
- `toLogged`: Boolean indicating whether the change is to LOGGED (true) or UNLOGGED (false)
## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelationName
  - [GetRelationPublications](../G/GetRelationPublications.md)
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [relation_open](../r/relation_open.md)
  - RelationIsPermanent
  - [relation_close](../r/relation_close.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)

- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (ALTER TABLE command preparation phase)

## Notes and Other Information
- Returns false if the operation is a no-op, true if the change should proceed
- Uses different scan strategies depending on the direction of the change (conrelid vs confrelid)
- Temporary tables cannot have their persistence changed and will result in an error
- Unlogged tables cannot be part of publications due to replication limitations  
- The function maintains PostgreSQL's invariant that permanent tables cannot reference unlogged tables
- Uses AccessShareLock when examining constraint and foreign table information
- Self-referencing foreign keys are explicitly allowed and ignored during validation
- [Publication](../P/Publication.md) membership is only checked when changing to UNLOGGED since that's the restriction

## Simplified Source

```c
static bool
ATPrepChangePersistence(Relation rel, bool toLogged)
{
    // Check if table is temporary (not allowed to change persistence)
    switch (rel->rd_rel->relpersistence) {
        case RELPERSISTENCE_TEMP:
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot change logged status of table \"%s\" because it is temporary",
                                  RelationGetRelationName(rel))));
            break;
        case RELPERSISTENCE_PERMANENT:
            if (toLogged) return false;  // Already logged, no-op
            break;
        case RELPERSISTENCE_UNLOGGED:
            if (!toLogged) return false;  // Already unlogged, no-op
            break;
    }

    // Check publication membership (unlogged tables can't be published)
    if (!toLogged && GetRelationPublications(RelationGetRelid(rel)) != NIL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("cannot change table \"%s\" to unlogged because it is part of a publication",
                              RelationGetRelationName(rel))));

    // Check foreign key constraints to maintain referential integrity
    Relation pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple tuple;

    // Scan for foreign key constraints (direction depends on operation)
    ScanKeyInit(&skey[0],
               toLogged ? Anum_pg_constraint_conrelid : Anum_pg_constraint_confrelid,
               BTEqualStrategyNumber, F_OIDEQ,
               ObjectIdGetDatum(RelationGetRelid(rel)));

    scan = systable_beginscan(pg_constraint,
                             toLogged ? ConstraintRelidTypidNameIndexId : InvalidOid,
                             true, NULL, 1, skey);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);

        if (con->contype == CONSTRAINT_FOREIGN) {
            Oid foreignrelid = toLogged ? con->confrelid : con->conrelid;

            // Skip self-referencing constraints
            if (RelationGetRelid(rel) == foreignrelid)
                continue;

            Relation foreignrel = relation_open(foreignrelid, AccessShareLock);

            // Validate persistence compatibility
            if (toLogged) {
                // Permanent tables can't reference unlogged tables
                if (!RelationIsPermanent(foreignrel))
                    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                   errmsg("could not change table \"%s\" to logged because it references unlogged table \"%s\"",
                                          RelationGetRelationName(rel), RelationGetRelationName(foreignrel))));
            } else {
                // Unlogged tables can't reference permanent tables
                if (RelationIsPermanent(foreignrel))
                    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                   errmsg("could not change table \"%s\" to unlogged because it references logged table \"%s\"",
                                          RelationGetRelationName(rel), RelationGetRelationName(foreignrel))));
            }

            relation_close(foreignrel, AccessShareLock);
        }
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    return true;  // Operation should proceed
}
```