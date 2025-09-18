# make_postgres

## Location
[src/bin/initdb/initdb.c:2047-2079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2047-L2079)

## Overview
Creates the postgres database during PostgreSQL initialization by copying template1 and configuring it as the default administrative connection database.

## Definition
```c
static void make_postgres(FILE *cmdfd)
```

## Detailed Description
The `make_postgres` function creates the postgres database during the initdb process. This database serves as the default administrative connection database for PostgreSQL. Like template0, it:

1. Creates the postgres database by copying from template1
2. Assigns a fixed OID (PostgresDbOid) for pg_upgrade compatibility
3. Uses the file_copy strategy for efficient initialization
4. Adds a descriptive comment identifying its purpose

The postgres database is typically the default database that administrators and applications connect to when no specific database is specified. It provides a standard entry point for database operations.

## Parameters / Member Variables
- `cmdfd`: File descriptor for writing SQL commands to be executed by the PostgreSQL server

## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PUTS (macro for writing SQL commands)
  - CppAsString2 (macro for converting PostgresDbOid to string)
  - PostgresDbOid (constant defining the fixed OID for postgres database)

- Called from:
  - [initialize_data_directory](../i/initialize_data_directory.md) (main initialization function in initdb)

## Notes and Other Information
- The postgres database serves as the default administrative connection database
- Like template0, it uses a fixed OID assignment for pg_upgrade compatibility
- The file_copy strategy is used for the same performance reasons as with template0
- This database is created after template0 in the initialization sequence
- Unlike template databases, the postgres database is meant for regular use and connections