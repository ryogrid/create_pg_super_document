# GetDatabaseTupleByOid

## Location
[src/backend/utils/init/postinit.c:144-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L144-L189)

## Overview
GetDatabaseTupleByOid is a static function that fetches the pg_database row for a specified database OID during backend startup when system catalogs are not yet fully accessible.

## Definition
static HeapTuple GetDatabaseTupleByOid(Oid dboid)

## Detailed Description
This function is the OID-based counterpart to GetDatabaseTuple, used during PostgreSQL backend startup to retrieve database information from the pg_database system catalog. Like GetDatabaseTuple, it operates when the backend doesn't yet have full access to system catalogs and can work in two modes:

1. **Index scan mode**: When criticalSharedRelcachesBuilt is true, it uses the database OID index (DatabaseOidIndexId) for efficient lookup
2. **Sequential scan mode**: When critical shared relcaches aren't available, it falls back to a sequential scan

The function opens the pg_database relation, performs a scan to find the tuple matching the given database OID, and returns a copy of the tuple to ensure it remains valid after the relation is closed.

## Parameters / Member Variables
- : The OID of the database to look up in pg_database catalog

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md) (for setting up scan key)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (for converting database OID to datum)
  - [table_open](../t/table_open.md) (for opening pg_database relation)
  - [systable_beginscan](../s/systable_beginscan.md) (for starting system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (for getting next tuple from scan)
  - [heap_copytuple](../h/heap_copytuple.md) (for copying tuple before releasing buffer)
  - [systable_endscan](../s/systable_endscan.md) (for ending scan)
  - [table_close](../t/table_close.md) (for closing relation)
- Called from:
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:1088)

## Notes and Other Information
- This is a static function, only accessible within postinit.c
- Similar to GetDatabaseTuple but searches by OID instead of name
- Uses DatabaseOidIndexId index when available, vs DatabaseNameIndexId used by GetDatabaseTuple
- Uses F_OIDEQ comparison function and Anum_pg_database_oid attribute
- The function uses AccessShareLock when opening the pg_database relation
- Returns a copied tuple to ensure the data remains valid after the relation buffer is released
- Returns NULL (invalid HeapTuple) if the specified database OID is not found
- Critical for database startup process, especially when switching databases by OID

## Simplified Source

```c
// Simplified version of GetDatabaseTupleByOid
static HeapTuple GetDatabaseTupleByOid(Oid dboid) {
    HeapTuple tuple;
    Relation relation;
    SysScanDesc scan;
    ScanKeyData key[1];

    // Set up scan key to search by database OID
    ScanKeyInit(&key[0],
                Anum_pg_database_oid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(dboid));

    // Open pg_database relation for reading
    relation = table_open(DatabaseRelationId, AccessShareLock);

    // Start scan (uses OID index if available, otherwise sequential scan)
    scan = systable_beginscan(relation, DatabaseOidIndexId,
                              criticalSharedRelcachesBuilt,
                              NULL, 1, key);

    // Get the matching tuple
    tuple = systable_getnext(scan);

    // Copy tuple to ensure it survives after buffer release
    if (HeapTupleIsValid(tuple))
        tuple = heap_copytuple(tuple);

    // Clean up scan and close relation
    systable_endscan(scan);
    table_close(relation, AccessShareLock);

    return tuple;
}
```

Key simplifications made:
- Added clear comments for each main operation
- Removed detailed comments about scan modes and kept essential logic
- Preserved the critical tuple copying logic and proper resource cleanup
- Maintained the adaptive scan strategy using DatabaseOidIndexId instead of DatabaseNameIndexId