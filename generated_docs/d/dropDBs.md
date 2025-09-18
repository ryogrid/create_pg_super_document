# dropDBs

## Location
[src/bin/pg_dump/pg_dumpall.c:1439-1485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1439-L1485)

## Overview
Generates DROP DATABASE statements for all user databases in a PostgreSQL cluster, excluding system databases and those that don't allow connections.

## Definition
static void dropDBs(PGconn *conn)

## Detailed Description
This function is part of the pg_dumpall utility's clean restoration process, generating SQL commands to drop all user-created databases in preparation for restoring from a dump. It carefully filters databases to only include those that allow connections (datallowconn = true) and have proper connection limits (datconnlimit != -2), ensuring compatibility with the dumpDatabases() function's selection criteria.

The function explicitly excludes critical system databases (postgres, template0, template1) that require special handling during restoration. It respects the global if_exists flag to generate conditional DROP DATABASE statements when safer operation is desired. This coordinated approach ensures that database dropping and recreation operations work together seamlessly.

## Parameters / Member Variables
- conn: PostgreSQL connection handle used to execute queries against the database

## Dependencies
- Functions called/Symbols referenced:
  - [executeQuery](../e/executeQuery.md): Executes SQL query to retrieve database information
  - [fmtId](../f/fmtId.md): Formats database names as proper SQL identifiers
- Called from (representative examples):
  - [main](../m/main.md): Primary entry point in pg_dumpall utility during clean restoration operations

## Notes and Other Information
- Only drops databases that allow connections and have valid connection limits
- Explicitly excludes system databases: postgres, template0, template1
- Uses the global if_exists variable to determine whether to include IF EXISTS clause
- Must coordinate with dumpDatabases() function to ensure consistent database selection
- Part of the clean restoration process where existing databases are dropped before recreation
- Essential for scenarios where target cluster must exactly match source cluster state
- Outputs descriptive header comments for better organization of dump files