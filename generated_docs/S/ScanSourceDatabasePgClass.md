# ScanSourceDatabasePgClass

## Location
[src/backend/commands/dbcommands.c:250-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L250-L327)

## Overview
ScanSourceDatabasePgClass scans the pg_class system catalog table in a source database to identify all relations that need to be copied to the destination database during database creation.

## Definition


## Detailed Description
This function performs a low-level scan of the pg_class relation in the source database, which is an exception to the usual rule that cross-database access is not possible. The function works by:

1. Obtaining the relfilenumber for pg_class using RelationMapOidToFilenumberForDatabase
2. Acquiring an AccessShareLock on the pg_class relation to ensure safe access
3. Opening the storage manager for the relation and determining the number of blocks
4. Using a bulk read access strategy for efficient sequential scanning
5. Obtaining a snapshot that sees all committed transactions as committed
6. Processing each block of the pg_class relation using ReadBufferWithoutRelcache
7. For each non-empty page, calling ScanSourceDatabasePgClassPage to extract relevant tuples
8. Building and returning a list of relations that need to be copied

The function bypasses the normal relcache and heap scan infrastructure since it's accessing a database to which PostgreSQL is not connected. This requires careful snapshot management and direct buffer access.

## Parameters / Member Variables
- : Tablespace ID of the source database's default tablespace
- : Database ID of the source database being scanned
- : Filesystem path to the source database directory

## Dependencies
- Functions called/Symbols referenced:
  - [RelationMapOidToFilenumberForDatabase](../R/RelationMapOidToFilenumberForDatabase.md): Maps relation OID to file number for specific database
  - [LockRelationId](../L/LockRelationId.md)/UnlockRelationId: Acquires and releases relation locks
  - [smgropen](../s/smgropen.md)/smgrclose/smgrnblocks: Storage manager operations for file access
  - GetAccessStrategy: Obtains buffer access strategy for bulk operations
  - GetLatestSnapshot: Gets snapshot that sees all committed transactions
  - [ReadBufferWithoutRelcache](../R/ReadBufferWithoutRelcache.md): Reads buffer without using relation cache
  - [ScanSourceDatabasePgClassPage](ScanSourceDatabasePgClassPage.md): Processes individual pages of pg_class
  - [PageIsNew](../P/PageIsNew.md)/PageIsEmpty: Checks page state
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md): Uses this to get list of relations to copy

## Notes and Other Information
- This function violates normal PostgreSQL rules about cross-database access, but is safe because the source database has no active connections
- Bypasses relcache and heap scan infrastructure due to cross-database nature
- Uses direct buffer access with ReadBufferWithoutRelcache instead of normal heap scanning
- Employs bulk read strategy for performance optimization during sequential scan
- Requires careful snapshot management to ensure consistent view of committed transactions
- Returns a list of CreateDBRelInfo structures representing relations to be copied
- Located at src/backend/commands/dbcommands.c:250-327