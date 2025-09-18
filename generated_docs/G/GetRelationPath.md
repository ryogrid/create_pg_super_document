# GetRelationPath

## Location
src/common/relpath.c: 141 - 210

## Overview
Constructs the complete filesystem path to a relation's file, handling different tablespaces, fork types, and temporary file naming conventions used by PostgreSQL's storage system.

## Definition
```c
char *GetRelationPath(Oid dbOid, Oid spcOid, RelFileNumber relNumber, int procNumber, ForkNumber forkNumber)
```

## Detailed Description
This function generates the full filesystem path for a relation file based on its storage location and characteristics. It handles three storage scenarios: global tablespace for shared system relations, default tablespace for regular database objects, and custom tablespaces accessed via symbolic links. The function also manages different fork types (main, fsm, vm, init) and supports temporary file naming when a process number is specified. The path construction varies based on whether the file is a main fork or auxiliary fork, and whether it's a temporary file created by a specific backend process.

## Parameters / Member Variables
- `dbOid`: Object ID of the database containing the relation (0 for global tablespace)
- `spcOid`: Object ID of the tablespace where the relation is stored
- `relNumber`: File number of the relation (RelFileNumber)
- `procNumber`: Process number for temporary files (INVALID_PROC_NUMBER for permanent files)
- `forkNumber`: Type of fork (MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, INIT_FORKNUM)

## Dependencies
- Functions called/Symbols referenced:
  - GLOBALTABLESPACE_OID (constant for global tablespace)
  - DEFAULTTABLESPACE_OID (constant for default tablespace)
  - INVALID_PROC_NUMBER (constant indicating no process number)
  - MAIN_FORKNUM (constant for main fork)
  - TABLESPACE_VERSION_DIRECTORY (version-specific directory name)
  - forkNames (array of fork name strings)
  - psprintf (PostgreSQL formatted string creation)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - GetIncrementalFilePath (in src/backend/backup/basebackup_incremental.c:634)
  - relpathbackend (via macro in src/include/common/relpath.h:86)
  - FORKNAMECHARS (referenced in src/include/common/relpath.h:76)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Must maintain consistency with GetDatabasePath() for proper file system layout
- Global tablespace files: 'global/{relNumber}' or 'global/{relNumber}_{forkName}'
- Default tablespace files: 'base/{dbOid}/{relNumber}' or 'base/{dbOid}/{relNumber}_{forkName}'
- Custom tablespace files: 'pg_tblspc/{spcOid}/{version}/{dbOid}/{relNumber}' or with fork suffix
- Temporary files include process number: 't{procNumber}_{relNumber}' format
- Main fork files don't include fork suffix, other forks append '_{forkName}'
- procNumber parameter typed as int rather than ProcNumber to avoid header dependencies