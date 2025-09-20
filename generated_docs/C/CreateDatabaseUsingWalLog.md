# CreateDatabaseUsingWalLog

## Location
[src/backend/commands/dbcommands.c:148-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L148-L249)

## Overview
CreateDatabaseUsingWalLog creates a new database by copying from a source database using the WAL_LOG strategy, where each copied block is individually written to the write-ahead log.

## Definition

```c
static void
CreateDatabaseUsingWalLog(Oid src_dboid, Oid dst_dboid,
						  Oid src_tsid, Oid dst_tsid)
```
## Detailed Description
This function implements the WAL_LOG strategy for database creation, which involves copying all relation data from a source database to a destination database with each copied block being separately logged to the write-ahead log. The function performs the following key operations:

1. Obtains source and destination database paths using GetDatabasePath
2. Creates the destination database directory and PG_VERSION file via CreateDirAndVersionFile
3. Copies the relation mapping file from source to destination using RelationMapCopy
4. Scans the source database's pg_class to get a list of all relfilelocators to copy
5. For each relation, acquires appropriate locks and copies the relation data using CreateAndCopyRelationData
6. Handles tablespace mapping, ensuring relations in the source db's default tablespace are created in the destination db's default tablespace

The function ensures proper locking to prevent concurrent modifications and handles cleanup of allocated memory and acquired locks.

## Parameters / Member Variables
- : Object ID of the source database to copy from
- : Object ID of the destination database being created
- : Tablespace ID of the source database's default tablespace
- : Tablespace ID of the destination database's default tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabasePath](../G/GetDatabasePath.md): Gets filesystem path for database directory
  - [CreateDirAndVersionFile](CreateDirAndVersionFile.md): Creates database directory and version file
  - [RelationMapCopy](../R/RelationMapCopy.md): Copies relation mapping file between databases
  - [ScanSourceDatabasePgClass](../S/ScanSourceDatabasePgClass.md): Scans pg_class to get list of relations to copy
  - [LockRelationId](../L/LockRelationId.md)/UnlockRelationId: Acquires and releases relation locks
  - CreateAndCopyRelationData: Copies actual relation data between locators
  - [list_free_deep](../l/list_free_deep.md): Frees allocated list memory
- Called from (representative examples):
  - [createdb](../c/createdb.md): Main database creation function in dbcommands.c

## Notes and Other Information
- This is a static function within dbcommands.c, not exposed publicly
- The WAL_LOG strategy logs each copied block individually, making it safer but potentially slower than bulk copy methods
- Proper locking is maintained even though both source and destination databases are locked at a higher level, following conservative practices
- Tablespace handling ensures that relations are correctly placed in appropriate tablespaces in the destination database
- Memory cleanup is performed for allocated paths and relation lists
- Located at src/backend/commands/dbcommands.c:148-249