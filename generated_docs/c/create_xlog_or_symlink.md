# create_xlog_or_symlink

## Location
[src/bin/initdb/initdb.c:2933-3015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2933-L3015)

## Overview
Creates the PostgreSQL Write-Ahead Log (WAL) directory, either as a regular subdirectory or as a symbolic link to an external location specified by the -X option during initdb.

## Definition

```c
void
create_xlog_or_symlink(void)
```
## Detailed Description
This function handles the creation of the  directory within the PostgreSQL data directory during database initialization. It supports two operational modes:

1. **External WAL Directory Mode** (when  is specified via -X option):
   - Validates that the specified path is absolute using  and 
   - Checks the state of the target directory using 
   - Creates the directory if it doesn't exist (case 0) or fixes permissions on existing empty directory (case 1)
   - Terminates initialization if the directory is non-empty (cases 2-4)
   - Creates a symbolic link from  to the external directory using 

2. **Standard WAL Directory Mode** (when no -X option is used):
   - Simply creates  as a regular directory using 

The function ensures proper permissions are set and provides appropriate error handling and user feedback throughout the process.

## Parameters / Member Variables
This function operates on global variables:
- : Global variable containing the external WAL directory path (NULL if not specified)
- : Global variable containing the PostgreSQL data directory path
- : Global variable specifying directory creation permissions
- : Global flag indicating if a new external WAL directory was created
- : Global flag indicating if an existing external WAL directory was used

## Dependencies
- Functions called/Symbols referenced:
  - : Formats and allocates string for subdirectory location
  - : Normalizes the external WAL directory path
  - : Validates that the WAL directory path is absolute
  - : Checks directory status and contents
  - : Creates directory with parent directories
  - : Prints success message
  - : Provides mount point warnings
  - : Logs detailed error hints
  - : Creates symbolic link to external WAL directory
  - : Creates regular WAL directory
  - : Changes directory permissions
  -                total        used        free      shared  buff/cache   available
Mem:        32819380     4943728    25403508        3040     2472144    27493428
Swap:        8388608           0     8388608: Deallocates allocated memory
- Called from (representative examples):
  - : Called during main initialization sequence

## Notes and Other Information
- The pg_wal directory is critical for PostgreSQL's Write-Ahead Logging mechanism
- External WAL directories enable performance optimization by placing WAL files on different storage devices
- The function enforces that external WAL directories must use absolute paths for reliability
- Similar validation logic to  ensures directory consistency
- The symbolic link approach allows transparent access to external WAL storage
- Mount point detection helps prevent WAL placement on inappropriate filesystem boundaries
- Memory cleanup with  ensures no resource leaks
- Global flags set by this function influence cleanup operations in case of initialization failure
- Proper permissions are essential for WAL security and proper database operation

## Simplified Source

```c
void
create_xlog_or_symlink(void)
{
    char *subdirloc;

    // Form path for pg_wal subdirectory/symlink
    subdirloc = psprintf("%s/pg_wal", pg_data);

    if (xlog_dir) {
        // External WAL directory mode (-X option specified)
        int ret;

        // Validate and canonicalize external WAL directory path
        canonicalize_path(xlog_dir);
        if (!is_absolute_path(xlog_dir))
            pg_fatal("WAL directory location must be an absolute path");

        // Check state of external WAL directory
        switch ((ret = pg_check_dir(xlog_dir)))
        {
            case 0:
                // Directory doesn't exist - create it
                printf(_("creating directory %s ... "), xlog_dir);
                fflush(stdout);

                if (pg_mkdir_p(xlog_dir, pg_dir_create_mode) != 0)
                    pg_fatal("could not create directory \"%s\": %m", xlog_dir);
                else
                    check_ok();

                made_new_xlogdir = true;
                break;

            case 1:
                // Directory exists but is empty - fix permissions
                printf(_("fixing permissions on existing directory %s ... "), xlog_dir);
                fflush(stdout);

                if (chmod(xlog_dir, pg_dir_create_mode) != 0)
                    pg_fatal("could not change permissions of directory \"%s\": %m", xlog_dir);
                else
                    check_ok();

                found_existing_xlogdir = true;
                break;

            case 2:
            case 3:
            case 4:
                // Directory exists and is not empty - error
                pg_log_error("directory \"%s\" exists but is not empty", xlog_dir);
                if (ret != 4)
                    warn_on_mount_point(ret);
                else
                    pg_log_error_hint("If you want to store the WAL there, either remove or empty the directory \"%s\".",
                                      xlog_dir);
                exit(1);

            default:
                // Cannot access directory
                pg_fatal("could not access directory \"%s\": %m", xlog_dir);
        }

        // Create symbolic link from pg_wal to external directory
        if (symlink(xlog_dir, subdirloc) != 0)
            pg_fatal("could not create symbolic link \"%s\": %m", subdirloc);
    }
    else {
        // Standard mode - create pg_wal as regular subdirectory
        if (mkdir(subdirloc, pg_dir_create_mode) < 0)
            pg_fatal("could not create directory \"%s\": %m", subdirloc);
    }

    free(subdirloc);
}
```