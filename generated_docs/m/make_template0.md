# make_template0

## Location
src/bin/initdb/initdb.c: 1993 - 2046

## Overview
Creates the template0 database during PostgreSQL initialization by copying template1 and configuring it as an unmodifiable empty database template.

## Definition
```c
static void make_template0(FILE *cmdfd)
```

## Detailed Description
The `make_template0` function is responsible for creating the template0 database during the initdb process. It creates template0 by copying from template1 using the file_copy strategy and then configures it with specific properties:

1. Sets template0 as a template database that disallows connections
2. Assigns a fixed OID (Template0DbOid) to ensure compatibility with pg_upgrade
3. Unsets collation version to disable collation version checks when creating new databases from template0
4. Updates template1's collation version to the actual version
5. Revokes public create and temporary privileges from both template databases
6. Adds a descriptive comment and performs cleanup

The function uses the file_copy strategy instead of wal_log during initdb because checkpoints are cheap at this stage, and it generates less WAL for a slightly faster and smaller cluster initialization.

## Parameters / Member Variables
- `cmdfd`: File descriptor for writing SQL commands to be executed by the PostgreSQL server

## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PUTS (macro for writing SQL commands)
  - CppAsString2 (macro for converting Template0DbOid to string)
  - Template0DbOid (constant defining the fixed OID for template0)

- Called from:
  - initialize_data_directory (main initialization function in initdb)

## Notes and Other Information
- template0 is designed to be an unmodifiable empty database that serves as a pristine template
- The fixed OID assignment is crucial for pg_upgrade compatibility to avoid OID conflicts
- The function ensures template0 has no collation-dependent objects by unsetting datcollversion
- Both template databases have public privileges revoked for security
- The file_copy strategy is preferred during initdb for performance reasons