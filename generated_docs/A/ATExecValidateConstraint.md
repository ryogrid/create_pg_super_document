# ATExecValidateConstraint

## Location
[src/backend/commands/tablecmds.c:11704-11892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11704-L11892)

## Overview
ATExecValidateConstraint implements the ALTER TABLE VALIDATE CONSTRAINT command, which validates a previously created NOT VALID constraint by checking all existing data against the constraint and marking it as validated in the catalog.

## Definition

```c
static ObjectAddress
ATExecValidateConstraint(List **wqueue, Relation rel, char *constrName,
						 bool recurse, bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function validates foreign key and check constraints that were previously created with the NOT VALID option. It finds the target constraint, verifies it's an appropriate type (foreign key or check), and if not already validated, queues the validation work for phase 3 of ALTER TABLE processing. The function handles both foreign key and check constraints differently:

For foreign key constraints:
- Creates a NewConstraint entry and queues it for validation
- Does not handle recursion since invalid foreign keys on partitioned tables are disallowed

For check constraints:
- Recursively validates child table constraints first to avoid deadlocks
- Requires all child constraints to be validated before parent validation
- Extracts the constraint expression and queues validation work

The function updates the constraint catalog entry to mark it as validated only after queueing the validation work.

## Parameters / Member Variables
- `**wqueue`: Work queue for ALTER TABLE operations to add validation tasks
- `rel`: The relation containing the constraint to validate
- `*constrName`: Name of the constraint to validate
- `recurse`: Whether to recursively validate constraints on child tables
- `recursing`: Whether this call is part of a recursive operation
- `lockmode`: Lock mode to use when accessing child relations
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - makeNode
  - [palloc0](../p/palloc0.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [lappend](../l/lappend.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [ATExecValidateConstraint](ATExecValidateConstraint.md) (recursive self-call)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)
  - [ATExecValidateConstraint](ATExecValidateConstraint.md) (recursive calls for child table constraints)

## Notes and Other Information
- Only works with foreign key and check constraints; other constraint types result in an error
- Validation is queued for phase 3 processing rather than performed immediately
- For check constraints with inheritance, all child constraints must be validated first
- Returns InvalidObjectAddress if the constraint was already validated
- The actual constraint checking is deferred to phase 3 to avoid holding locks too long
- Handles recursion at this level rather than phase 1 to optimize locking for foreign keys

## Simplified Source

```c
static ObjectAddress
ATExecValidateConstraint(List **wqueue, Relation rel, char *constrName,
                         bool recurse, bool recursing, LOCKMODE lockmode)
{
    Relation conrel;
    SysScanDesc scan;
    ScanKeyData skey[3];
    HeapTuple tuple;
    Form_pg_constraint con;
    ObjectAddress address;

    // Open constraint catalog
    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    // Find the target constraint by relation, type, and name
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&skey[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(InvalidOid));
    ScanKeyInit(&skey[2], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(constrName));
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, NULL, 3, skey);

    // Check constraint exists
    if (!HeapTupleIsValid(tuple = systable_getnext(scan)))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("constraint \"%s\" of relation \"%s\" does not exist",
                       constrName, RelationGetRelationName(rel))));

    con = (Form_pg_constraint) GETSTRUCT(tuple);

    // Verify constraint type (only foreign key and check constraints supported)
    if (con->contype != CONSTRAINT_FOREIGN && con->contype != CONSTRAINT_CHECK)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("constraint \"%s\" is not a foreign key or check constraint",
                       constrName)));

    // If already validated, return early
    if (con->convalidated) {
        address = InvalidObjectAddress;
    } else {
        AlteredTableInfo *tab;
        NewConstraint *newcon;

        if (con->contype == CONSTRAINT_FOREIGN) {
            // Handle foreign key constraint validation
            Constraint *fkconstraint = makeNode(Constraint);
            fkconstraint->conname = constrName;

            newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
            newcon->name = constrName;
            newcon->contype = CONSTR_FOREIGN;
            newcon->refrelid = con->confrelid;
            newcon->refindid = con->conindid;
            newcon->conid = con->oid;
            newcon->qual = (Node *) fkconstraint;

        } else if (con->contype == CONSTRAINT_CHECK) {
            // Handle check constraint validation with inheritance
            if (!recursing && !con->connoinherit) {
                List *children = find_all_inheritors(RelationGetRelid(rel), lockmode, NULL);
                ListCell *child;

                // Recursively validate child constraints first
                foreach(child, children) {
                    Oid childoid = lfirst_oid(child);
                    Relation childrel;

                    if (childoid == RelationGetRelid(rel))
                        continue;

                    if (!recurse)
                        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                errmsg("constraint must be validated on child tables too")));

                    childrel = table_open(childoid, NoLock);
                    ATExecValidateConstraint(wqueue, childrel, constrName, false, true, lockmode);
                    table_close(childrel, NoLock);
                }
            }

            // Setup check constraint validation
            newcon = (NewConstraint *) palloc0(sizeof(NewConstraint));
            newcon->name = constrName;
            newcon->contype = CONSTR_CHECK;
            newcon->conid = con->oid;

            // Extract constraint expression
            Datum val = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conbin);
            char *conbin = TextDatumGetCString(val);
            newcon->qual = (Node *) stringToNode(conbin);

            CacheInvalidateRelcache(rel);
        }

        // Queue validation work for phase 3
        tab = ATGetQueueEntry(wqueue, rel);
        tab->constraints = lappend(tab->constraints, newcon);

        // Mark constraint as validated in catalog
        HeapTuple copyTuple = heap_copytuple(tuple);
        Form_pg_constraint copy_con = (Form_pg_constraint) GETSTRUCT(copyTuple);
        copy_con->convalidated = true;
        CatalogTupleUpdate(conrel, &copyTuple->t_self, copyTuple);

        InvokeObjectPostAlterHook(ConstraintRelationId, con->oid, 0);
        heap_freetuple(copyTuple);

        ObjectAddressSet(address, ConstraintRelationId, con->oid);
    }

    systable_endscan(scan);
    table_close(conrel, RowExclusiveLock);

    return address;
}
```