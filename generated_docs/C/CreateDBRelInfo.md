# CreateDBRelInfo

## Location
[src/backend/commands/dbcommands.c:104-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L104-L109)

## Overview
CreateDBRelInfo is a structure that holds information about a relation to be copied when creating a database in PostgreSQL.

## Definition


## Detailed Description
CreateDBRelInfo is a struct used during database creation operations to track information about relations that need to be copied from a source database to a new database. This structure is part of PostgreSQL's database creation mechanism, specifically used in scenarios where databases are created using WAL (Write-Ahead Log) operations or when scanning and copying existing database structures.

The structure encapsulates the essential information needed to identify and handle a relation during the database creation process, including its physical storage location, logical identifier, and persistence characteristics.

## Parameters / Member Variables
- : A RelFileLocator that provides the physical relation identifier, specifying where the relation's files are stored in the filesystem
- : The relation's Object Identifier (OID), which uniquely identifies the relation within the database catalog
- : A boolean flag indicating whether the relation is permanent (persistent across database restarts) or unlogged (data is not written to WAL and is lost on crash)

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (referenced as member type)
  - Oid (referenced as member type)
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](CreateDatabaseUsingWalLog.md) (at src/backend/commands/dbcommands.c:159)
  - [ScanSourceDatabasePgClassPage](../S/ScanSourceDatabasePgClassPage.md) (at src/backend/commands/dbcommands.c:364, 390)
  - [ScanSourceDatabasePgClassTuple](../S/ScanSourceDatabasePgClassTuple.md) (at src/backend/commands/dbcommands.c:394, 432)

## Notes and Other Information
- This structure is defined in src/backend/commands/dbcommands.c (lines 104-109)
- It is specifically used during database creation operations that involve copying relations from existing databases
- The structure is used by functions that scan the pg_class catalog to identify relations that need to be copied
- The permanent flag is crucial for determining how the relation should be handled during the copy operation, as unlogged tables have different replication and recovery characteristics
- This is an internal structure used within the database command subsystem and is not exposed to user-level operations