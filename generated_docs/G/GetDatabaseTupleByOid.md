# GetDatabaseTupleByOid

## Location
src/backend/utils/init/postinit.c: 144 - 189

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
  - table_open (for opening pg_database relation)
  - [systable_beginscan](../s/systable_beginscan.md) (for starting system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (for getting next tuple from scan)
  - [heap_copytuple](../h/heap_copytuple.md) (for copying tuple before releasing buffer)
  - [systable_endscan](../s/systable_endscan.md) (for ending scan)
  - table_close (for closing relation)
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