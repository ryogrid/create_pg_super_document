# AlterConstraintNamespaces

## Location
[src/backend/catalog/pg_constraint.c:755-823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_constraint.c#L755-L823)

## Overview
Moves all constraints belonging to a specified object (relation or domain type) from one namespace to another during namespace change operations.

## Definition
void AlterConstraintNamespaces(Oid ownerId, Oid oldNspId, Oid newNspId, bool isType, ObjectAddresses *objsMoved)

## Detailed Description
AlterConstraintNamespaces is responsible for updating the namespace of constraints when their owning objects are moved between schemas. The function performs a comprehensive scan of the pg_constraint catalog to locate all constraints associated with the specified owner object and updates their namespace accordingly.

The function handles both relation constraints and domain type constraints by using conditional logic based on the isType parameter. It scans using a composite key that searches either conrelid or contypid depending on the object type. The function ensures constraints are only moved if they haven't already been processed (tracked via objsMoved) and only if a namespace change is actually needed.

## Parameters / Member Variables
- : The OID of the relation or type that owns the constraints to be moved
- : The OID of the current namespace the constraints belong to
- : The OID of the target namespace to move constraints to  
- : Boolean flag indicating whether the owner is a domain type (true) or relation (false)
- : ObjectAddresses structure tracking already-processed objects to prevent duplicates

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - ObjectAddressSet
  - [object_address_present](../o/object_address_present.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md) (tablecmds.c:17303)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md) (typecmds.c:4261, 4268)

## Notes and Other Information
- Uses ConstraintRelidTypidNameIndexId index for efficient constraint lookup
- Only updates constraints that actually belong to the old namespace to avoid unnecessary work
- Constraints don't maintain their own namespace dependencies, so changeDependencyFor() is not needed
- Post-alter hooks are invoked for all processed constraints regardless of whether they were updated
- The objsMoved parameter prevents duplicate processing when multiple objects reference the same constraints
- Designed to work seamlessly with schema alteration operations for both tables and domain types

## Simplified Source

```c
void
AlterConstraintNamespaces(Oid ownerId, Oid oldNspId, Oid newNspId,
                          bool isType, ObjectAddresses *objsMoved)
{
    Relation conRel;
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;

    // Open pg_constraint catalog with exclusive lock
    conRel = table_open(ConstraintRelationId, RowExclusiveLock);

    // Set up scan keys based on object type
    ScanKeyInit(&key[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(isType ? InvalidOid : ownerId));
    ScanKeyInit(&key[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(isType ? ownerId : InvalidOid));

    // Scan for constraints belonging to the object
    scan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId, true, NULL, 2, key);

    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Form_pg_constraint conform = (Form_pg_constraint) GETSTRUCT(tup);
        ObjectAddress thisobj;

        ObjectAddressSet(thisobj, ConstraintRelationId, conform->oid);

        // Skip if already processed
        if (object_address_present(&thisobj, objsMoved))
            continue;

        // Update namespace if needed
        if (conform->connamespace == oldNspId && oldNspId != newNspId) {
            tup = heap_copytuple(tup);
            conform = (Form_pg_constraint) GETSTRUCT(tup);
            conform->connamespace = newNspId;

            // Update the catalog tuple
            CatalogTupleUpdate(conRel, &tup->t_self, tup);
        }

        // Invoke post-alter hooks and track object
        InvokeObjectPostAlterHook(ConstraintRelationId, thisobj.objectId, 0);
        add_exact_object_address(&thisobj, objsMoved);
    }

    systable_endscan(scan);
    table_close(conRel, RowExclusiveLock);
}
```