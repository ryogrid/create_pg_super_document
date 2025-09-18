# LibraryInfo

## Location
src/bin/pg_upgrade/pg_upgrade.h: 336 - 350

## Overview
LibraryInfo is a structure that stores information about loadable libraries in PostgreSQL clusters during the upgrade process.

## Definition
```c
typedef struct
{
    char       *name;
    int         dbnum;
} LibraryInfo;
```

## Detailed Description
The LibraryInfo structure represents metadata about shared libraries that are loaded into PostgreSQL databases. This structure is used by pg_upgrade to track and validate loadable libraries across different database clusters during the upgrade process. Each LibraryInfo entry contains the library name and an associated database number, enabling pg_upgrade to ensure library compatibility between old and new PostgreSQL versions.

## Parameters / Member Variables
- `name`: String pointer containing the name of the loadable library
- `dbnum`: Integer identifier representing the database number associated with this library

## Dependencies
- Functions called/Symbols referenced:
  - Referenced within ClusterInfo structure
- Called from (representative examples):
  - library_name_compare (in function.c)
  - get_loadable_libraries (in function.c) 
  - check_loadable_libraries (in function.c)
  - OSInfo structure (as libraries array)

## Notes and Other Information
- This structure is typically used in arrays to represent all loadable libraries in a cluster
- The pg_upgrade utility uses this information to verify that all required libraries are available in the target PostgreSQL version
- Library compatibility checking is crucial for successful upgrades, as missing or incompatible libraries can cause upgrade failures
- The `dbnum` field helps associate libraries with specific databases within a cluster