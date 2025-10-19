# make_template0

## Location
[src/bin/initdb/initdb.c:1993-2046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1993-L2046)

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
  - [initialize_data_directory](../i/initialize_data_directory.md) (main initialization function in initdb)

## Notes and Other Information
- template0 is designed to be an unmodifiable empty database that serves as a pristine template
- The fixed OID assignment is crucial for pg_upgrade compatibility to avoid OID conflicts
- The function ensures template0 has no collation-dependent objects by unsetting datcollversion
- Both template databases have public privileges revoked for security
- The file_copy strategy is preferred during initdb for performance reasons

## Simplified Source

```c
static void make_template0(FILE *cmdfd) {
    // Create template0 database with fixed OID for pg_upgrade compatibility
    // Use file_copy strategy for better performance during initdb
    PG_CMD_PUTS("CREATE DATABASE template0 IS_TEMPLATE = true ALLOW_CONNECTIONS = false"
                " OID = " CppAsString2(Template0DbOid)
                " STRATEGY = file_copy;\n\n");

    // Clear collation version to disable version checks for new databases
    PG_CMD_PUTS("UPDATE pg_database SET datcollversion = NULL WHERE datname = 'template0';\n\n");

    // Set proper collation version on template1
    PG_CMD_PUTS("UPDATE pg_database SET datcollversion = pg_database_collation_actual_version(oid) WHERE datname = 'template1';\n\n");

    // Revoke public privileges for security
    PG_CMD_PUTS("REVOKE CREATE,TEMPORARY ON DATABASE template1 FROM public;\n\n");
    PG_CMD_PUTS("REVOKE CREATE,TEMPORARY ON DATABASE template0 FROM public;\n\n");

    // Add descriptive comment and cleanup
    PG_CMD_PUTS("COMMENT ON DATABASE template0 IS 'unmodifiable empty database';\n\n");
    PG_CMD_PUTS("VACUUM pg_database;\n\n");
}
```