# create_xlog_or_symlink

## Location
[src/bin/initdb/initdb.c:2933-3015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2933-L3015)

## Overview
Creates the PostgreSQL Write-Ahead Log (WAL) directory, either as a regular subdirectory or as a symbolic link to an external location specified by the -X option during initdb.

## Definition


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