# RelationMapFinishBootstrap

## Location
[src/backend/utils/cache/relmapper.c:625-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L625-L650)

## Overview
Writes out the initial relation mapping files at the completion of PostgreSQL's bootstrap process.

## Definition

```c
void
RelationMapFinishBootstrap(void)
```
## Detailed Description
The RelationMapFinishBootstrap function is called at the end of PostgreSQL's bootstrap phase to create the initial relation mapping files on disk. During bootstrap, the system catalogs and other mapped relations are created and their mappings are established in memory. This function persists those mappings to the filesystem.

The function performs several important tasks:

1. **Validation**: Asserts that the process is indeed in bootstrap mode using IsBootstrapProcessingMode()
2. **State Verification**: Confirms that there are no pending or active mapping updates, as all mappings should have been established during bootstrap via RelationMapUpdateMap calls
3. **File Creation**: Writes both the shared mapping file (for shared catalogs) and local mapping file (for database-specific catalogs) using write_relmap_file()

Key characteristics of the bootstrap mapping file creation:
- **No WAL logging**: Bootstrap operations don't use WAL since recovery isn't needed
- **No cache invalidation**: No sinval messages are sent since no other processes exist yet
- **No file preservation**: No need to preserve files since we're creating the initial state
- **Exclusive locking**: Uses RelationMappingLock to ensure exclusive access during file creation

The function creates two mapping files:
- **Shared map**: Contains mappings for shared catalogs (stored in global tablespace)
- **Local map**: Contains mappings for database-specific catalogs (stored in the database's default tablespace)

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode (macro to verify bootstrap mode)
  - LWLockAcquire (acquiring RelationMappingLock in LW_EXCLUSIVE mode)
  - LWLockRelease (releasing RelationMappingLock)
  - [write_relmap_file](../w/write_relmap_file.md) (called twice - once for shared map, once for local map)
  - Assert (for debugging verification of state)
- Global variables accessed:
  - shared_map (static RelMapFile structure containing shared catalog mappings)
  - local_map (static RelMapFile structure containing local catalog mappings)
  - active_shared_updates (verified to be empty)
  - active_local_updates (verified to be empty)
  - pending_shared_updates (verified to be empty)
  - pending_local_updates (verified to be empty)
  - MyDatabaseId (database OID for local mapping file)
  - MyDatabaseTableSpace (tablespace OID for local mapping file)
  - DatabasePath (file system path for local mapping file)
- Called from (representative examples):
  - [BootstrapModeMain](../B/BootstrapModeMain.md) (in src/backend/bootstrap/bootstrap.c)

## Notes and Other Information
- This function is only called once during the entire lifecycle of a PostgreSQL cluster - at the end of initdb
- The mappings written by this function form the foundation for all subsequent relation mapping operations
- Bootstrap mode is a special PostgreSQL startup mode used only during initial cluster creation
- The function assumes that all necessary catalog mappings have been established in memory during the bootstrap process
- No error recovery is needed since bootstrap is an all-or-nothing operation
- The mapping files created here are critical for PostgreSQL to be able to locate its own system catalogs on subsequent startups
- Future mapping changes will use the normal transactional update mechanisms rather than this bootstrap-specific function