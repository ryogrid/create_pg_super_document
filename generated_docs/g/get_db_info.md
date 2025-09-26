# get_db_info

## Location
[src/backend/commands/dbcommands.c:2781-2938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2781-L2938)

## Overview
get_db_info is a static helper function that looks up comprehensive information about a database by name, optionally acquiring a lock on the database, and fills in requested database attributes.

## Definition

```c
static bool
get_db_info(const char *name, LOCKMODE lockmode,
			Oid *dbIdP, Oid *ownerIdP,
			int *encodingP, bool *dbIsTemplateP, bool *dbAllowConnP, bool *dbHasLoginEvtP,
			TransactionId *dbFrozenXidP, MultiXactId *dbMinMultiP,
			Oid *dbTablespace, char **dbCollate, char **dbCtype, char **dbLocale,
			char **dbIcurules,
			char *dbLocProvider,
			char **dbCollversion)
```
## Detailed Description
This function provides a centralized way to lookup database information from the pg_database catalog table. The function:

1. Opens the pg_database relation with AccessShareLock
2. Searches for the database by name using a system table scan (no syscache available for name-based lookups)
3. If found, optionally acquires the specified lock on the database using its OID
4. Re-fetches the database tuple by OID to handle potential race conditions (database rename between lookup and lock)
5. Extracts and returns all requested database attributes through output parameters
6. Handles concurrent database renames by retrying the lookup if the name changes during the process
7. Returns true if the database exists and was successfully processed, false otherwise

The function supports extracting a comprehensive set of database properties including basic metadata, encoding settings, template status, connection permissions, transaction IDs, tablespace, and various locale/collation settings.

## Parameters / Member Variables
- `name`: Database name to look up
- `lockmode`: Lock mode to acquire on the database (NoLock for no locking)
- `dbIdP`: Output parameter for database OID
- `ownerIdP`: Output parameter for database owner OID
- `encodingP`: Output parameter for character encoding
- `dbIsTemplateP`: Output parameter for template database flag
- `dbAllowConnP`: Output parameter for connection allowed flag
- `dbHasLoginEvtP`: Output parameter for login event trigger flag
- `dbFrozenXidP`: Output parameter for frozen transaction ID
- `dbMinMultiP`: Output parameter for minimum MultiXactId
- `dbTablespace`: Output parameter for default tablespace OID
- `dbCollate`: Output parameter for collation setting
- `dbCtype`: Output parameter for character classification setting
- `dbLocale`: Output parameter for locale setting
- `dbIcurules`: Output parameter for ICU rules
- `dbLocProvider`: Output parameter for locale provider type
- `dbCollversion`: Output parameter for collation version

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [LockSharedObject](../L/LockSharedObject.md)/UnlockSharedObject
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)/SysCacheGetAttrNotNull
  - TextDatumGetCString
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [createdb](../c/createdb.md)
  - [dropdb](../d/dropdb.md)
  - [RenameDatabase](../R/RenameDatabase.md)
  - [movedb](../m/movedb.md)

## Notes and Other Information
- Static function used internally within dbcommands.c for database operations
- Handles race conditions by retrying lookup if database is renamed during the process
- Uses system table scan rather than syscache since no name-based cache exists for databases
- All output parameters are optional - callers can pass NULL for unneeded information
- Properly manages locks and cache references to prevent resource leaks
- Central point for database attribute extraction used by multiple database management commands
- Returns false if database doesn't exist, allowing callers to handle missing databases appropriately