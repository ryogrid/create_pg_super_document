# FdwInfo

## Location
src/bin/pg_dump/pg_dump.h: 573 - 574

## Overview
FdwInfo is a structure used in pg_dump to represent a PostgreSQL foreign data wrapper (FDW), storing metadata needed to dump and restore foreign data wrappers.

## Definition
```c
typedef struct _fdwInfo
{
    DumpableObject dobj;
    DumpableAcl dacl;
    const char *rolname;
    char       *fdwhandler;
    char       *fdwvalidator;
    char       *fdwoptions;
} FdwInfo;
```

## Detailed Description
FdwInfo is part of pg_dump's internal representation of PostgreSQL database objects that need to be dumped and restored. It specifically handles foreign data wrappers, which are PostgreSQL extensions that allow access to external data sources as if they were regular PostgreSQL tables. The structure stores the handler and validator functions, access control information, ownership details, and configuration options. This information is retrieved from the pg_foreign_data_wrapper system catalog and used to generate CREATE FOREIGN DATA WRAPPER statements during database dumps.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common metadata (name, namespace, dump ID, object type)
- `dacl`: Access control list (ACL) information for the foreign data wrapper
- `rolname`: Name of the role that owns this foreign data wrapper
- `fdwhandler`: Name of the handler function that implements the FDW interface
- `fdwvalidator`: Name of the validator function that validates FDW options (can be NULL)
- `fdwoptions`: String containing the foreign data wrapper options in key-value format

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - DumpableAcl (access control list structure)
- Called from (representative examples):
  - [getForeignDataWrappers](../g/getForeignDataWrappers.md) (populates FdwInfo structures from pg_foreign_data_wrapper catalog)
  - [dumpForeignDataWrapper](../d/dumpForeignDataWrapper.md) (uses FdwInfo to generate CREATE FOREIGN DATA WRAPPER statements)
  - fmtQualifiedDumpable (formats the wrapper name for output)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_dump.h:565-573
- Used exclusively within pg_dump for backing up and restoring foreign data wrappers
- The structure maps directly to columns in the pg_foreign_data_wrapper system catalog
- Foreign data wrappers are the foundation of PostgreSQL's foreign data access functionality
- The fdwhandler function is required and implements the core FDW functionality
- The fdwvalidator function is optional and validates options passed to the FDW
- Part of PostgreSQL's SQL/MED (Management of External Data) implementation
- FDW options are stored as a formatted string that gets parsed during restoration