# recordMultipleDependencies

## Location
[src/backend/catalog/pg_depend.c:58-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L58-L193)

## Overview
Records multiple dependencies of the same type for a single dependent object efficiently, using batch insertion to minimize overhead compared to recording each dependency separately.

## Definition

```c
void
recordMultipleDependencies(const ObjectAddress *depender,
						   const ObjectAddress *referenced,
						   int nreferenced,
						   DependencyType behavior)
```
## Detailed Description
This function provides an optimized way to record multiple dependency relationships for a single dependent object. It creates entries in the pg_depend system catalog table using batch insertion techniques to improve performance when dealing with multiple dependencies. The function handles several optimizations including skipping pinned objects (which don't need dependency tracking), using tuple slots for efficient insertion, and batching insertions to reduce I/O overhead. It also handles bootstrap mode by returning early since pg_depend may not exist during system initialization.

## Parameters / Member Variables
- `*depender`: Pointer to ObjectAddress of the dependent object (the one that depends on others)
- `*referenced`: Pointer to array of ObjectAddress structures representing the referenced objects
- `nreferenced`: Integer count of how many referenced objects are in the array
- `behavior`: DependencyType enum value specifying the type of dependency relationship for all entries
## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [table_open](../t/table_open.md)
  - [isObjectPinned](../i/isObjectPinned.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md)
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - MAX_CATALOG_MULTI_INSERT_BYTES
  - DependencyType
  - [CatalogIndexState](../C/CatalogIndexState.md)
  - [CharGetDatum](../C/CharGetDatum.md)
- Called from (representative examples):
  - [recordDependencyOnExpr](recordDependencyOnExpr.md)
  - [recordDependencyOnSingleRelExpr](recordDependencyOnSingleRelExpr.md)
  - [record_object_address_dependencies](record_object_address_dependencies.md)
  - [recordDependencyOn](recordDependencyOn.md)

## Notes and Other Information
- Located in src/backend/catalog/pg_depend.c:58-193
- Uses batch insertion with configurable slot buffer size based on MAX_CATALOG_MULTI_INSERT_BYTES
- Automatically skips pinned objects to save space in pg_depend catalog
- Returns early during bootstrap processing mode since pg_depend may not exist
- Does not check for duplicate dependencies - allows them without harm
- Opens indexes lazily only when actually needed for insertion
- Uses efficient tuple slot management with proper cleanup
- Optimized for cases where multiple objects depend on the same set of referenced objects

## Simplified Source

```c
void recordMultipleDependencies(const ObjectAddress *depender,
                               const ObjectAddress *referenced,
                               int nreferenced,
                               DependencyType behavior)
{
    // Early returns for edge cases
    if (nreferenced <= 0)
        return;

    if (IsBootstrapProcessingMode())
        return;

    // Open pg_depend table
    Relation dependDesc = table_open(DependRelationId, RowExclusiveLock);

    // Calculate batch size and allocate slots
    int max_slots = Min(nreferenced,
                       MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_depend));
    TupleTableSlot **slot = palloc(sizeof(TupleTableSlot *) * max_slots);

    CatalogIndexState indstate = NULL;
    int slot_stored_count = 0;
    int slot_init_count = 0;

    // Process each referenced object
    for (int i = 0; i < nreferenced; i++, referenced++) {
        // Skip pinned objects (no dependency tracking needed)
        if (isObjectPinned(referenced))
            continue;

        // Initialize slot if needed
        if (slot_init_count < max_slots) {
            slot[slot_stored_count] = MakeSingleTupleTableSlot(
                RelationGetDescr(dependDesc), &TTSOpsHeapTuple);
            slot_init_count++;
        }

        ExecClearTuple(slot[slot_stored_count]);

        // Fill tuple slot with dependency data
        slot[slot_stored_count]->tts_values[Anum_pg_depend_refclassid - 1] =
            ObjectIdGetDatum(referenced->classId);
        slot[slot_stored_count]->tts_values[Anum_pg_depend_refobjid - 1] =
            ObjectIdGetDatum(referenced->objectId);
        slot[slot_stored_count]->tts_values[Anum_pg_depend_refobjsubid - 1] =
            Int32GetDatum(referenced->objectSubId);
        slot[slot_stored_count]->tts_values[Anum_pg_depend_deptype - 1] =
            CharGetDatum((char) behavior);
        slot[slot_stored_count]->tts_values[Anum_pg_depend_classid - 1] =
            ObjectIdGetDatum(depender->classId);
        slot[slot_stored_count]->tts_values[Anum_pg_depend_objid - 1] =
            ObjectIdGetDatum(depender->objectId);
        slot[slot_stored_count]->tts_values[Anum_pg_depend_objsubid - 1] =
            Int32GetDatum(depender->objectSubId);

        // Mark all values as not null
        memset(slot[slot_stored_count]->tts_isnull, false,
               slot[slot_stored_count]->tts_tupleDescriptor->natts * sizeof(bool));

        ExecStoreVirtualTuple(slot[slot_stored_count]);
        slot_stored_count++;

        // Batch insert when slots are full
        if (slot_stored_count == max_slots) {
            if (indstate == NULL)
                indstate = CatalogOpenIndexes(dependDesc);

            CatalogTuplesMultiInsertWithInfo(dependDesc, slot,
                                           slot_stored_count, indstate);
            slot_stored_count = 0;
        }
    }

    // Insert remaining tuples
    if (slot_stored_count > 0) {
        if (indstate == NULL)
            indstate = CatalogOpenIndexes(dependDesc);

        CatalogTuplesMultiInsertWithInfo(dependDesc, slot,
                                       slot_stored_count, indstate);
    }

    // Cleanup
    if (indstate != NULL)
        CatalogCloseIndexes(indstate);

    table_close(dependDesc, RowExclusiveLock);

    for (int i = 0; i < slot_init_count; i++)
        ExecDropSingleTupleTableSlot(slot[i]);
    pfree(slot);
}
```