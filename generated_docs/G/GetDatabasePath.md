# GetDatabasePath

## Location
src/common/relpath.c: 110 - 140

## Overview
Constructs the filesystem path to a database directory based on database OID and tablespace OID, handling different storage locations including global tablespace, default tablespace, and custom tablespaces.

## Definition
```c
char *GetDatabasePath(Oid dbOid, Oid spcOid)
```

## Detailed Description
This function generates the appropriate filesystem path for a database directory within PostgreSQL's data directory structure. It handles three distinct cases: global tablespace for shared system relations (stored in 'global' directory), default tablespace for regular databases (stored under 'base' directory), and custom tablespaces (accessed via symbolic links under 'pg_tblspc'). The function returns a palloc'd string that must be freed by the caller. The path construction logic must remain consistent with GetRelationPath() to ensure proper file system organization.

## Parameters / Member Variables
- `dbOid`: Object ID of the database (0 for global tablespace)
- `spcOid`: Object ID of the tablespace where the database resides

## Dependencies
- Functions called/Symbols referenced:
  - GLOBALTABLESPACE_OID (constant for global tablespace)
  - DEFAULTTABLESPACE_OID (constant for default tablespace)
  - TABLESPACE_VERSION_DIRECTORY (version-specific directory name)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - [psprintf](../p/psprintf.md) (PostgreSQL formatted string creation)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md) (in src/backend/commands/dbcommands.c:162-163)
  - [CreateDatabaseUsingFileCopy](../C/CreateDatabaseUsingFileCopy.md) (in src/backend/commands/dbcommands.c:589, 604)
  - [createdb](../c/createdb.md) (in src/backend/commands/dbcommands.c:1317)
  - [movedb](../m/movedb.md) (in src/backend/commands/dbcommands.c:2070-2071)
  - [InitPostgres](../I/InitPostgres.md) (in src/backend/utils/init/postinit.c:1162)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Must maintain consistency with GetRelationPath() for proper file system layout
- Global tablespace (spcOid == GLOBALTABLESPACE_OID) requires dbOid to be 0
- Default tablespace path format: 'base/{dbOid}'
- Custom tablespace path format: 'pg_tblspc/{spcOid}/{version}/{dbOid}'
- Used extensively in database creation, movement, and WAL replay operations