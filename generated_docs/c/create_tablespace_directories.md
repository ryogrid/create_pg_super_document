# create_tablespace_directories

## Location
[src/backend/commands/tablespace.c:572-685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L572-L685)

## Overview
Creates the filesystem infrastructure for a tablespace by establishing directory structures and symlinks between $PGDATA/pg_tblspc/ and the specified location.

## Definition

```c
struct stat st;
```
## Detailed Description
create_tablespace_directories establishes the physical filesystem infrastructure required for a tablespace to function. The function creates a versioned directory structure at the target location and establishes a symbolic link from the PostgreSQL data directory to enable tablespace access.

The function handles two operational modes: normal tablespaces with symbolic links and 'in-place' tablespaces (developer feature) that create directories directly in the data directory. It validates target directory permissions, creates the required version directory to prevent location conflicts, and manages symlink creation with special handling for WAL recovery scenarios.

Permission validation ensures the target directory is accessible and properly secured. The version directory serves as a unique marker preventing multiple tablespaces from using the same location. During recovery, the function removes stale symlinks before creating new ones.

## Parameters / Member Variables
- : Filesystem path where the tablespace should be created
- : OID of the tablespace for generating the symlink name

## Dependencies
- Functions called/Symbols referenced:
  - [MakePGDirectory](../M/MakePGDirectory.md): Creates directories with proper PostgreSQL permissions
  - TABLESPACE_VERSION_DIRECTORY: Constant defining the version subdirectory name
  - S_ISDIR: System macro to verify directory status
  - [remove_tablespace_symlink](../r/remove_tablespace_symlink.md): Removes existing symlinks during recovery
  - symlink: System call to create symbolic links
- Called from (representative examples):
  - [CreateTableSpace](../C/CreateTableSpace.md): During tablespace creation
  - [tblspc_redo](../t/tblspc_redo.md): During WAL replay for tablespace creation

## Notes and Other Information
- Supports both normal and 'in-place' tablespace creation modes
- Validates and sets appropriate permissions on target directories
- Creates version directory to prevent location conflicts between tablespaces
- Handles special cases during WAL recovery including stale symlink removal
- Uses pg_dir_create_mode for consistent directory permissions
- Prevents multiple tablespaces from sharing the same physical location
- Provides different error messages and hints depending on recovery context

## Simplified Source

```c
static void create_tablespace_directories(const char *location, const Oid tablespaceoid) {
    char *linkloc;
    char *location_with_version_dir;
    struct stat st;
    bool in_place;

    // Generate symlink location path
    linkloc = psprintf("pg_tblspc/%u", tablespaceoid);

    // Check if this is an 'in-place' tablespace (developer option)
    in_place = strlen(location) == 0;

    // For in-place tablespaces, create directory directly
    if (in_place) {
        if (MakePGDirectory(linkloc) < 0 && errno != EEXIST) {
            ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not create directory \"%s\": %m", linkloc)));
        }
    }

    // Build path to version directory
    location_with_version_dir = psprintf("%s/%s",
                                        in_place ? linkloc : location,
                                        TABLESPACE_VERSION_DIRECTORY);

    // Set permissions on target directory (if not in-place)
    if (!in_place && chmod(location, pg_dir_create_mode) != 0) {
        if (errno == ENOENT) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FILE),
                 errmsg("directory \"%s\" does not exist", location),
                 InRecovery ? errhint("Create this directory for the tablespace before "
                                     "restarting the server.") : 0));
        } else {
            ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not set permissions on directory \"%s\": %m", location)));
        }
    }

    // Create or validate version directory
    if (stat(location_with_version_dir, &st) < 0) {
        if (errno != ENOENT) {
            ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not stat directory \"%s\": %m", location_with_version_dir)));
        } else if (MakePGDirectory(location_with_version_dir) < 0) {
            ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not create directory \"%s\": %m", location_with_version_dir)));
        }
    } else if (!S_ISDIR(st.st_mode)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
             errmsg("\"%s\" exists but is not a directory", location_with_version_dir)));
    } else if (!InRecovery) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
             errmsg("directory \"%s\" already in use as a tablespace", location_with_version_dir)));
    }

    // During recovery, remove old symlink if it exists
    if (!in_place && InRecovery)
        remove_tablespace_symlink(linkloc);

    // Create symbolic link to tablespace location
    if (!in_place && symlink(location, linkloc) < 0) {
        ereport(ERROR,
            (errcode_for_file_access(),
             errmsg("could not create symbolic link \"%s\": %m", linkloc)));
    }

    // Cleanup allocated memory
    pfree(linkloc);
    pfree(location_with_version_dir);
}
```