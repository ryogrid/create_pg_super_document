# TablespaceCreateDbspace

## Location
[src/backend/commands/tablespace.c:112-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L112-L207)

## Overview
Creates database-specific subdirectories within tablespaces to isolate each database's objects into its own namespace, handling both normal operation and WAL replay scenarios.

## Definition

```c
struct stat st;
```
## Detailed Description
TablespaceCreateDbspace ensures that each database using a tablespace is isolated into its own namespace by creating a subdirectory named for the database OID. The function handles both normal operations and WAL replay scenarios, with special logic to cope with missing directories during recovery.

The function performs atomic directory creation using TablespaceCreateLock to prevent race conditions with concurrent DROP TABLESPACE operations. During WAL replay (isRedo=true), it employs a more permissive approach, creating directory hierarchies as needed to handle cases where tablespaces may have been dropped ahead in the WAL stream.

For the global tablespace (GLOBALTABLESPACE_OID), the function returns early as it doesn't require per-database subdirectories.

## Parameters / Member Variables
- : The OID of the tablespace where the database subdirectory should be created
- : The OID of the database for which to create the subdirectory
- : Boolean flag indicating whether this is being called during WAL replay, which affects error handling behavior

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabasePath](../G/GetDatabasePath.md): Constructs the full path for the database directory
  - S_ISDIR: System macro to check if a file is a directory
  - [MakePGDirectory](../M/MakePGDirectory.md): PostgreSQL wrapper for creating directories
  - [pg_mkdir_p](../p/pg_mkdir_p.md): Creates directory hierarchies recursively
- Called from (representative examples):
  - [mdcreate](../m/mdcreate.md): During relation file creation

## Notes and Other Information
- Uses TablespaceCreateLock (LW_EXCLUSIVE) to ensure atomic directory creation
- During WAL replay, employs fallback strategies for missing directory hierarchies
- Global tablespace is exempt from per-database subdirectory creation
- Performs double-checked locking pattern to avoid unnecessary work
- Error handling differs between normal operation and WAL replay modes

## Simplified Source

```c
void TablespaceCreateDbspace(Oid spcOid, Oid dbOid, bool isRedo) {
    struct stat st;
    char *dir;

    // Global tablespace doesn't need per-database subdirectories
    if (spcOid == GLOBALTABLESPACE_OID)
        return;

    Assert(OidIsValid(spcOid));
    Assert(OidIsValid(dbOid));

    // Get the path for this database in the tablespace
    dir = GetDatabasePath(dbOid, spcOid);

    if (stat(dir, &st) < 0) {
        // Directory doesn't exist
        if (errno == ENOENT) {
            // Acquire lock to prevent concurrent DROP TABLESPACE operations
            LWLockAcquire(TablespaceCreateLock, LW_EXCLUSIVE);

            // Double-check if directory was created while waiting for lock
            if (stat(dir, &st) == 0 && S_ISDIR(st.st_mode)) {
                // Directory was created by another process
            } else {
                // Try to create the directory
                if (MakePGDirectory(dir) < 0) {
                    // During WAL replay, handle missing parent directories
                    if (errno != ENOENT || !isRedo)
                        ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not create directory \"%s\": %m", dir)));

                    // Create directory hierarchy for WAL replay
                    if (pg_mkdir_p(dir, pg_dir_create_mode) < 0)
                        ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not create directory \"%s\": %m", dir)));
                }
            }

            LWLockRelease(TablespaceCreateLock);
        } else {
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not stat directory \"%s\": %m", dir)));
        }
    } else {
        // Path exists - verify it's a directory
        if (!S_ISDIR(st.st_mode))
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" exists but is not a directory", dir)));
    }

    pfree(dir);
}
```