# make_new_heap

## Location
[src/backend/commands/cluster.c:688-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L688-L813)

## Overview
Creates a transient table with the same logical structure as an existing table but with specified physical storage properties, used during CLUSTER, ALTER TABLE, and similar operations.

## Definition

```c
struct VacuumCutoffs cutoffs;
```
## Detailed Description
The `make_new_heap` function creates a temporary table that duplicates the logical structure (columns, data types) of an existing table while allowing different physical storage characteristics. This is a critical component of PostgreSQL's table reorganization operations.

Key aspects of the function:
1. Creates a new heap table with a temporary name ("pg_temp_" + original OID)
2. Preserves the original table's column structure and data types
3. Copies reloptions from the original table to maintain storage parameters
4. Creates an associated TOAST table if the original had one
5. Does not copy constraints, defaults, or indexes (these are rebuilt later)

The function handles both regular and temporary tables appropriately, placing temporary tables in the pg_temp namespace and preserving the mapped relation status when necessary.

## Parameters / Member Variables
- `OIDOldHeap`: OID of the original table whose structure should be duplicated
- `NewTableSpace`: OID of the tablespace where the new table should be created
- `NewAccessMethod`: OID of the access method (table AM) to use for the new table
- `relpersistence`: Persistence characteristic (permanent, temporary, unlogged)
- `lockmode`: Lock mode to acquire on the original table

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens the original relation
  - RelationGetDescr: Gets the table's tuple descriptor
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up relation information in system cache
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md): Retrieves relation options from cache
  - [LookupCreationNamespace](../L/LookupCreationNamespace.md): Finds appropriate namespace for temporary tables
  - RelationGetNamespace: Gets the namespace of the original relation
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md): Creates the new table with catalog entries
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Makes new catalog entries visible
  - [NewHeapCreateToastTable](../N/NewHeapCreateToastTable.md): Creates TOAST table if needed
  - [table_close](../t/table_close.md): Closes the original relation
- Called from (representative examples):
  - [rebuild_relation](../r/rebuild_relation.md): Part of clustering operation
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md): Materialized view refresh
  - [ATRewriteTables](../A/ATRewriteTables.md): Table rewriting during ALTER TABLE

## Notes and Other Information
- The new table does not inherit constraints, defaults, or indexes from the original
- Uses a naming convention of "pg_temp_" + original table OID to avoid conflicts
- Preserves storage options (reloptions) from the original table
- Handles TOAST tables appropriately, creating new TOAST relations when needed
- The mapped relation property is preserved for system catalogs like pg_class
- Returns the OID of the newly created table for use in subsequent operations
- Critical for maintaining data consistency during table reorganization operations

## Simplified Source

```c
Oid
make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
              char relpersistence, LOCKMODE lockmode)
{
    TupleDesc OldHeapDesc;
    char NewHeapName[NAMEDATALEN];
    Oid OIDNewHeap;
    Oid toastid;
    Relation OldHeap;
    HeapTuple tuple;
    Datum reloptions;
    bool isNull;
    Oid namespaceid;

    // Open the original table to get its structure
    OldHeap = table_open(OIDOldHeap, lockmode);
    OldHeapDesc = RelationGetDescr(OldHeap);

    // Get reloptions from the original table
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(OIDOldHeap));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);

    reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
    if (isNull)
        reloptions = (Datum) 0;

    // Determine the namespace for the new table
    if (relpersistence == RELPERSISTENCE_TEMP)
        namespaceid = LookupCreationNamespace("pg_temp");
    else
        namespaceid = RelationGetNamespace(OldHeap);

    // Create temporary name for the new heap
    snprintf(NewHeapName, sizeof(NewHeapName), "pg_temp_%u", OIDOldHeap);

    // Create the new heap table with catalog entries
    OIDNewHeap = heap_create_with_catalog(NewHeapName,
                                         namespaceid,
                                         NewTableSpace,
                                         InvalidOid,        // no type OID
                                         InvalidOid,        // no array type
                                         InvalidOid,        // no toast type
                                         OldHeap->rd_rel->relowner,
                                         NewAccessMethod,
                                         OldHeapDesc,       // same column structure
                                         NIL,               // no constraints
                                         RELKIND_RELATION,
                                         relpersistence,
                                         false,             // not shared
                                         RelationIsMapped(OldHeap), // preserve mapped status
                                         ONCOMMIT_NOOP,
                                         reloptions,        // preserve storage options
                                         false,             // no OID column
                                         true,              // allow system columns
                                         true,              // valid relfilenode
                                         OIDOldHeap,        // relation being rewritten
                                         NULL);             // no typeName

    ReleaseSysCache(tuple);

    // Make the new table visible for subsequent operations
    CommandCounterIncrement();

    // Create TOAST table if the original had one
    toastid = OldHeap->rd_rel->reltoastrelid;
    if (OidIsValid(toastid)) {
        // Get TOAST table's reloptions
        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for relation %u", toastid);

        reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
        if (isNull)
            reloptions = (Datum) 0;

        // Create TOAST table for new heap
        NewHeapCreateToastTable(OIDNewHeap, reloptions, lockmode, toastid);

        ReleaseSysCache(tuple);
    }

    table_close(OldHeap, NoLock);

    return OIDNewHeap;
}
```