# copyTemplateDependencies

## Location
[src/backend/catalog/pg_shdepend.c:895-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L895-L998)

## Overview
Creates the initial shared dependencies for a new database by copying dependency records from a template database, establishing proper relationships with shared objects like roles and tablespaces.

## Definition
```c
void copyTemplateDependencies(Oid templateDbId, Oid newDbId)
```

## Detailed Description
This function establishes the shared dependency relationships for a newly created database by scanning all dependency entries associated with the template database and creating corresponding entries for the new database. It performs an efficient batch insertion process using tuple slots to minimize the performance impact of inserting potentially large numbers of dependency records. The function specifically excludes copying dependencies with dbId == 0 (shared objects), which prevents copying the ownership dependency of the template database itself - a desired behavior to avoid inappropriate ownership relationships.

## Parameters / Member Variables
- `templateDbId`: OID of the template database from which to copy dependencies
- `newDbId`: OID of the new database that will receive the copied dependencies

## Dependencies
- Functions called/Symbols referenced:
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md) (creates tuple slots for batch operations)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md) (cleans up tuple slots)
  - [ExecClearTuple](../E/ExecClearTuple.md) (clears tuple slot contents)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md) (stores tuple data in slot)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md) (opens catalog indexes for insertion)
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md) (closes catalog indexes)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md) (performs batch tuple insertion)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext (system catalog scanning functions)
  - Form_pg_shdepend (shared dependency tuple structure)
- Called from (representative examples):
  - [createdb](createdb.md) (database creation in dbcommands.c:1468)

## Notes and Other Information
- Uses batch insertion with configurable slot count based on MAX_CATALOG_MULTI_INSERT_BYTES to optimize performance
- Delays slot initialization until needed to avoid unnecessary memory allocation
- Copies all dependency fields except dbId, which is changed from templateDbId to newDbId
- Scans using SharedDependDependerIndexId for efficient template database dependency retrieval
- Excludes shared object dependencies (dbId == 0) to prevent inappropriate template database ownership copying
- Properly handles cleanup of allocated tuple slots to prevent memory leaks
- Uses RowExclusiveLock on the shared dependency relation during the copy operation
- Essential for database creation process to ensure new databases have proper relationships with shared objects

## Simplified Source

```c
void copyTemplateDependencies(Oid templateDbId, Oid newDbId) {
    Relation sdepRel;
    TupleDesc sdepDesc;
    SysScanDesc scan;
    HeapTuple tup;
    CatalogIndexState indstate;
    TupleTableSlot **slot;
    int max_slots, slot_init_count = 0, slot_stored_count = 0;

    // Open shared dependency catalog
    sdepRel = table_open(SharedDependRelationId, RowExclusiveLock);
    sdepDesc = RelationGetDescr(sdepRel);

    // Allocate slots for batch insertion
    max_slots = MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_shdepend);
    slot = palloc(sizeof(TupleTableSlot *) * max_slots);
    indstate = CatalogOpenIndexes(sdepRel);

    // Scan all shared dependencies for the template database
    ScanKeyInit(&key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(templateDbId));
    scan = systable_beginscan(sdepRel, SharedDependDependerIndexId, true, NULL, 1, key);

    // Copy each dependency entry, changing dbId to new database
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_shdepend shdep = (Form_pg_shdepend) GETSTRUCT(tup);

        // Initialize slot if needed
        if (slot_init_count < max_slots) {
            slot[slot_stored_count] = MakeSingleTupleTableSlot(sdepDesc, &TTSOpsHeapTuple);
            slot_init_count++;
        }

        // Clear and populate tuple slot with dependency data
        ExecClearTuple(slot[slot_stored_count]);
        memset(slot[slot_stored_count]->tts_isnull, false,
               sdepDesc->natts * sizeof(bool));

        // Copy all fields, but change dbId to new database
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_dbid - 1] = ObjectIdGetDatum(newDbId);
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_classid - 1] = shdep->classid;
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_objid - 1] = shdep->objid;
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_objsubid - 1] = shdep->objsubid;
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_refclassid - 1] = shdep->refclassid;
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_refobjid - 1] = shdep->refobjid;
        slot[slot_stored_count]->tts_values[Anum_pg_shdepend_deptype - 1] = shdep->deptype;

        ExecStoreVirtualTuple(slot[slot_stored_count]);
        slot_stored_count++;

        // Insert batch when slots are full
        if (slot_stored_count == max_slots) {
            CatalogTuplesMultiInsertWithInfo(sdepRel, slot, slot_stored_count, indstate);
            slot_stored_count = 0;
        }
    }

    // Insert remaining tuples
    if (slot_stored_count > 0)
        CatalogTuplesMultiInsertWithInfo(sdepRel, slot, slot_stored_count, indstate);

    // Cleanup
    systable_endscan(scan);
    CatalogCloseIndexes(indstate);
    table_close(sdepRel, RowExclusiveLock);

    for (int i = 0; i < slot_init_count; i++)
        ExecDropSingleTupleTableSlot(slot[i]);
    pfree(slot);
}
```