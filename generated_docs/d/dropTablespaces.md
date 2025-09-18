# dropTablespaces

## Location
[src/bin/pg_dump/pg_dumpall.c:1300-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1300-L1334)

## Overview
Generates DROP TABLESPACE statements for all user-defined tablespaces in the PostgreSQL cluster, excluding built-in system tablespaces.

## Definition
static void dropTablespaces(PGconn *conn)

## Detailed Description
This function is part of the pg_dumpall utility and is responsible for generating SQL commands to drop all user-defined tablespaces in a PostgreSQL cluster. It specifically excludes built-in system tablespaces that have names starting with 'pg_' prefix. The function queries the pg_tablespace system catalog to retrieve all non-system tablespaces and generates DROP TABLESPACE statements for each one.

The function supports conditional dropping through the global if_exists flag, which when set will generate 'DROP TABLESPACE IF EXISTS' statements instead of plain 'DROP TABLESPACE' commands. This provides safer operation when the target cluster might not have all the same tablespaces.

## Parameters / Member Variables
- conn: PostgreSQL connection handle used to execute queries against the database

## Dependencies
- Functions called/Symbols referenced:
  - [executeQuery](../e/executeQuery.md): Executes SQL query to retrieve tablespace information
  - [fmtId](../f/fmtId.md): Formats tablespace names as proper SQL identifiers
- Called from (representative examples):
  - [main](../m/main.md): Primary entry point in pg_dumpall utility when performing clean restore operations

## Notes and Other Information
- Only processes user-defined tablespaces, filtering out system tablespaces with pg_ prefix
- Uses the global if_exists variable to determine whether to include IF EXISTS clause
- Outputs descriptive header comments in the dump file for organization
- Part of the cluster restoration process where existing objects need to be dropped before recreation
- Essential for clean database cluster restoration scenarios where target system needs to match source exactly