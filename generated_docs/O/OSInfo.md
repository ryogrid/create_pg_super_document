# OSInfo

## Location
src/bin/pg_upgrade/pg_upgrade.h: 351 - 393

## Overview
OSInfo is a structure that stores operating system and environment information used by the pg_upgrade utility during PostgreSQL cluster upgrades.

## Definition
```c
typedef struct
{
    const char *progname;        /* complete pathname for this program */
    char       *user;            /* username for clusters */
    bool        user_specified;  /* user specified on command-line */
    char      **old_tablespaces; /* tablespaces */
    int         num_old_tablespaces;
    LibraryInfo *libraries;      /* loadable libraries */
    int         num_libraries;
    ClusterInfo *running_cluster;
} OSInfo;
```

## Detailed Description
The OSInfo structure serves as a central repository for operating system and environment information that pg_upgrade needs during the cluster upgrade process. It contains program identification, user authentication details, tablespace information, library data, and references to running cluster information. This structure provides the environmental context necessary for pg_upgrade to safely and correctly perform PostgreSQL version upgrades.

## Parameters / Member Variables
- `progname`: Constant string pointer containing the complete pathname of the pg_upgrade program
- `user`: String pointer for the username under which clusters should run
- `user_specified`: Boolean flag indicating whether the user was explicitly specified on the command line
- `old_tablespaces`: Array of string pointers containing tablespace paths from the old cluster
- `num_old_tablespaces`: Integer count of tablespaces in the old_tablespaces array
- `libraries`: Pointer to array of LibraryInfo structures containing loadable library information
- `num_libraries`: Integer count of libraries in the libraries array
- `running_cluster`: Pointer to ClusterInfo structure representing the currently running cluster

## Dependencies
- Functions called/Symbols referenced:
  - [LibraryInfo](../L/LibraryInfo.md) (for libraries array)
  - ClusterInfo (for running_cluster)
  - Various pg_upgrade utility functions
- Called from (representative examples):
  - RESTORE_TRANSACTION_SIZE (in pg_upgrade.c)
  - Global variable os_info (in pg_upgrade.h)

## Notes and Other Information
- This structure is typically instantiated as a global variable (os_info) accessible throughout the pg_upgrade process
- The tablespace information is critical for ensuring proper data file location handling during upgrades
- Library tracking helps ensure all required shared libraries are available in the target PostgreSQL version
- The running_cluster reference provides access to live cluster information during the upgrade process
- User information ensures proper ownership and permissions are maintained during the upgrade