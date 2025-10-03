# ATExecDropConstraint

## Location
[src/backend/commands/tablecmds.c:12556-12806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12556-L12806)

## Overview
Executes ALTER TABLE DROP CONSTRAINT commands, handling constraint deletion with inheritance recursion and proper dependency management.

## Definition

```c
static void
ATExecDropConstraint(Relation rel, const char *constrName,
					 DropBehavior behavior,
					 bool recurse, bool recursing,
					 bool missing_ok, LOCKMODE lockmode)
```
## Detailed Description
This function implements constraint deletion for ALTER TABLE operations. Unlike normal ALTER TABLE recursion, it uses a custom recursion mechanism to properly handle inherited constraints. The function searches for the target constraint in pg_constraint, validates permissions, handles foreign key locking requirements, performs the actual deletion via the dependency system, and recursively processes child tables. It properly manages inheritance counts for CHECK constraints and handles both CASCADE and RESTRICT behaviors. For partitioned tables, it enforces that constraints cannot be dropped from only the parent when partitions exist.

## Parameters / Member Variables
- `rel`: The relation from which to drop the constraint
- `*constrName`: Name of the constraint to drop
- `behavior`: CASCADE or RESTRICT behavior for dependency handling
- `recurse`: Whether to recursively drop from child tables
- `recursing`: True when called recursively on child tables
- `missing_ok`: Whether to report error if constraint doesn't exist
- `lockmode`: Lock level to use when accessing child relations
## Dependencies
- Functions called/Symbols referenced:
  - [ATSimplePermissions](ATSimplePermissions.md) (permission checking)
  - [table_open](../t/table_open.md)/table_close (relation access)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan (catalog scanning)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md) (safety validation)
  - [performDeletion](../p/performDeletion.md) (dependency-based deletion)
  - [find_inheritance_children](../f/find_inheritance_children.md) (inheritance hierarchy)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (visibility control)
  - [heap_copytuple](../h/heap_copytuple.md)/heap_freetuple (tuple management)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE executor)
  - [ATExecDropConstraint](ATExecDropConstraint.md) (recursive self-calls)

## Notes and Other Information
- Cannot use normal ALTER TABLE recursion due to special inheritance handling requirements
- Prevents dropping inherited constraints unless recursing from parent
- For foreign key constraints, locks referenced table to prevent concurrent modifications
- CHECK constraints are handled with inheritance count management
- Non-CHECK constraints on partitioned tables are handled via dependency mechanism
- Supports IF EXISTS semantics via missing_ok parameter
- Uses custom one-level-at-a-time recursion for proper constraint inheritance handling

## Simplified Source

```c
static void
ATExecDropConstraint(Relation rel, const char *constrName,
                     DropBehavior behavior, bool recurse, bool recursing,
                     bool missing_ok, LOCKMODE lockmode)
{
    List *children;
    Relation conrel;
    Form_pg_constraint con;
    SysScanDesc scan;
    ScanKeyData skey[3];
    HeapTuple tuple;
    bool found = false;
    bool is_no_inherit_constraint = false;
    char contype;

    // Check permissions for recursive calls
    if (recursing)
        ATSimplePermissions(AT_DropConstraint, rel, ATT_TABLE | ATT_FOREIGN_TABLE);

    // Open constraint catalog
    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    // Search for the target constraint
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(constrName));

    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);

    if (HeapTupleIsValid(tuple = systable_getnext(scan)))
    {
        ObjectAddress conobj;
        con = (Form_pg_constraint) GETSTRUCT(tuple);

        // Validate constraint can be dropped
        if (con->coninhcount > 0 && !recursing)
            ereport(ERROR, (errmsg("cannot drop inherited constraint \"%s\"", constrName)));

        is_no_inherit_constraint = con->connoinherit;
        contype = con->contype;

        // For foreign keys, lock referenced table
        if (contype == CONSTRAINT_FOREIGN && con->confrelid != RelationGetRelid(rel))
        {
            Relation frel = table_open(con->confrelid, AccessExclusiveLock);
            CheckAlterTableIsSafe(frel);
            table_close(frel, NoLock);
        }

        // Delete the constraint via dependency system
        conobj.classId = ConstraintRelationId;
        conobj.objectId = con->oid;
        conobj.objectSubId = 0;
        performDeletion(&conobj, behavior, 0);
        found = true;
    }

    systable_endscan(scan);

    // Handle constraint not found
    if (!found)
    {
        if (missing_ok)
            ereport(NOTICE, (errmsg("constraint \"%s\" does not exist, skipping", constrName)));
        else
            ereport(ERROR, (errmsg("constraint \"%s\" of relation \"%s\" does not exist",
                                   constrName, RelationGetRelationName(rel))));
        table_close(conrel, RowExclusiveLock);
        return;
    }

    // For partitioned tables, non-CHECK constraints handled via dependencies
    if (contype != CONSTRAINT_CHECK && rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
    {
        table_close(conrel, RowExclusiveLock);
        return;
    }

    // Handle inheritance children
    if (!is_no_inherit_constraint)
        children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    else
        children = NIL;

    // Validate partitioned table constraints
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE && children != NIL && !recurse)
        ereport(ERROR, (errmsg("cannot remove constraint from only the partitioned table when partitions exist")));

    // Process each child relation
    Oid childrelid;
    foreach_oid(childrelid, children)
    {
        Relation childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        // Find constraint in child
        ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(childrelid));
        ScanKeyInit(&skey[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(InvalidOid));
        ScanKeyInit(&skey[2], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ,
                    CStringGetDatum(constrName));

        scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);
        tuple = systable_getnext(scan);

        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmsg("constraint \"%s\" of relation \"%s\" does not exist",
                                   constrName, RelationGetRelationName(childrel))));

        HeapTuple copy_tuple = heap_copytuple(tuple);
        systable_endscan(scan);

        con = (Form_pg_constraint) GETSTRUCT(copy_tuple);

        if (recurse)
        {
            // Either drop child constraint or decrement inheritance count
            if (con->coninhcount == 1 && !con->conislocal)
            {
                // Recursively drop from child
                ATExecDropConstraint(childrel, constrName, behavior, true, true, false, lockmode);
            }
            else
            {
                // Just decrement inheritance count
                con->coninhcount--;
                CatalogTupleUpdate(conrel, &copy_tuple->t_self, copy_tuple);
                CommandCounterIncrement();
            }
        }
        else
        {
            // Mark as local definition
            con->coninhcount--;
            con->conislocal = true;
            CatalogTupleUpdate(conrel, &copy_tuple->t_self, copy_tuple);
            CommandCounterIncrement();
        }

        heap_freetuple(copy_tuple);
        table_close(childrel, NoLock);
    }

    table_close(conrel, RowExclusiveLock);
}
```