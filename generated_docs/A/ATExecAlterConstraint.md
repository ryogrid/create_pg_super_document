# ATExecAlterConstraint

## Location
[src/backend/commands/tablecmds.c:11416-11552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11416-L11552)

## Overview
ATExecAlterConstraint updates the attributes of a constraint in PostgreSQL, specifically implementing the ALTER TABLE ALTER CONSTRAINT command. Currently it only works for Foreign Key constraints.

## Definition

```c
static ObjectAddress
ATExecAlterConstraint(Relation rel, AlterTableCmd *cmd, bool recurse,
					  bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function modifies constraint attributes such as deferrability and initial deferred status for foreign key constraints. It performs several validation checks including ensuring the constraint exists, is a foreign key constraint, and is a top-level constraint (not inherited). The function handles both regular tables and partitioned tables, with special logic for partitioned tables where partitions need processing regardless of whether the constraint attributes actually changed.

The function follows these main steps:
1. Opens the constraint and trigger system catalogs
2. Searches for the target constraint by name and relation
3. Validates the constraint type (must be foreign key)
4. Ensures it's a top-level constraint (not inherited from a parent)
5. Calls ATExecAlterConstrRecurse to perform the actual modification
6. Invalidates relation caches for affected relations

## Parameters / Member Variables
- `rel`: The relation containing the constraint to be altered
- `*cmd`: The ALTER TABLE command containing constraint modification details
- `recurse`: Whether to recursively apply changes to child tables
- `recursing`: Whether this call is part of a recursive operation
- `lockmode`: The lock mode to use for the operation
## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ATExecAlterConstrRecurse](ATExecAlterConstrRecurse.md)
  - ObjectAddressSet
  - [CacheInvalidateRelcacheByRelid](../C/CacheInvalidateRelcacheByRelid.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Only supports foreign key constraints; other constraint types will result in an error
- Inherited constraints cannot be altered directly - the user must alter the parent constraint instead
- For partitioned tables, all partitions are processed even if the constraint attributes don't change
- The function maintains referential integrity by invalidating caches for all affected relations
- Returns InvalidObjectAddress if no changes were made, otherwise returns the constraint's ObjectAddress

## Simplified Source

```c
static ObjectAddress
ATExecAlterConstraint(Relation rel, AlterTableCmd *cmd, bool recurse,
                     bool recursing, LOCKMODE lockmode)
{
    Constraint *cmdcon = castNode(Constraint, cmd->def);
    Relation conrel, tgrel;
    SysScanDesc scan;
    ScanKeyData skey[3];
    HeapTuple contuple;
    Form_pg_constraint currcon;
    ObjectAddress address;
    List *otherrelids = NIL;
    ListCell *lc;

    // Open constraint and trigger catalogs
    conrel = table_open(ConstraintRelationId, RowExclusiveLock);
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);

    // Find the target constraint by name
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname, BTEqualStrategyNumber,
                F_NAMEEQ, CStringGetDatum(cmdcon->conname));
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);

    // Validate constraint exists and is a foreign key
    if (!HeapTupleIsValid(contuple = systable_getnext(scan)))
        ereport(ERROR, "constraint \"%s\" of relation \"%s\" does not exist",
                cmdcon->conname, RelationGetRelationName(rel));

    currcon = (Form_pg_constraint) GETSTRUCT(contuple);
    if (currcon->contype != CONSTRAINT_FOREIGN)
        ereport(ERROR, "constraint \"%s\" is not a foreign key constraint",
                cmdcon->conname);

    // Ensure it's a top-level constraint (not inherited)
    if (OidIsValid(currcon->conparentid)) {
        // Find the topmost constraint in the inheritance hierarchy
        HeapTuple tp;
        Oid parent = currcon->conparentid;
        char *ancestorname = NULL;
        char *ancestortable = NULL;

        while (HeapTupleIsValid(tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent)))) {
            Form_pg_constraint contup = (Form_pg_constraint) GETSTRUCT(tp);
            if (!OidIsValid(contup->conparentid)) {
                ancestorname = pstrdup(NameStr(contup->conname));
                ancestortable = get_rel_name(contup->conrelid);
                ReleaseSysCache(tp);
                break;
            }
            parent = contup->conparentid;
            ReleaseSysCache(tp);
        }

        ereport(ERROR, "cannot alter constraint \"%s\" on relation \"%s\"",
                cmdcon->conname, RelationGetRelationName(rel));
    }

    // Perform the actual constraint modification if needed
    address = InvalidObjectAddress;
    if (currcon->condeferrable != cmdcon->deferrable ||
        currcon->condeferred != cmdcon->initdeferred ||
        rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {

        if (ATExecAlterConstrRecurse(cmdcon, conrel, tgrel, rel, contuple,
                                   &otherrelids, lockmode))
            ObjectAddressSet(address, ConstraintRelationId, currcon->oid);
    }

    // Invalidate relcache for relations with related triggers
    foreach(lc, otherrelids)
        CacheInvalidateRelcacheByRelid(lfirst_oid(lc));

    systable_endscan(scan);
    table_close(tgrel, RowExclusiveLock);
    table_close(conrel, RowExclusiveLock);

    return address;
}
```