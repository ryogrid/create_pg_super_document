# GetDatabaseTuple

## Location
[src/backend/utils/init/postinit.c:101-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L101-L143)

## Overview
GetDatabaseTuple is a static function that fetches the pg_database row for a specified database name during backend startup when system catalogs are not yet fully accessible.

## Definition
static HeapTuple GetDatabaseTuple(const char *dbname)

## Detailed Description
This function is used during PostgreSQL backend startup to retrieve database information from the pg_database system catalog when the backend doesn't yet have full access to system catalogs. The function can operate in two modes depending on whether critical shared relcache entries have been built:

1. **Index scan mode**: When criticalSharedRelcachesBuilt is true, it can use the database name index for efficient lookup
2. **Sequential scan mode**: When critical shared relcaches aren't available, it falls back to a sequential scan using only the hard-wired descriptor for pg_database

The function opens the pg_database relation, performs a scan to find the tuple matching the given database name, and returns a copy of the tuple to ensure it remains valid after the relation is closed.

## Parameters / Member Variables
- : The name of the database to look up in pg_database catalog

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md) (for setting up scan key)
  - [CStringGetDatum](../C/CStringGetDatum.md) (for converting database name to datum)
  - table_open (for opening pg_database relation)
  - [systable_beginscan](../s/systable_beginscan.md) (for starting system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (for getting next tuple from scan)
  - [heap_copytuple](../h/heap_copytuple.md) (for copying tuple before releasing buffer)
  - [systable_endscan](../s/systable_endscan.md) (for ending scan)
  - table_close (for closing relation)
- Called from:
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:1030)

## Notes and Other Information
- This is a static function, only accessible within postinit.c
- The function uses AccessShareLock when opening the pg_database relation
- It returns a copied tuple to ensure the data remains valid after the relation buffer is released
- The scan strategy depends on criticalSharedRelcachesBuilt global variable
- Returns NULL (invalid HeapTuple) if the specified database is not found
- Critical for database startup process before full catalog access is available