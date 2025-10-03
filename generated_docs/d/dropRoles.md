# dropRoles

## Location
[src/bin/pg_dump/pg_dumpall.c:740-786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L740-L786)

## Overview
The dropRoles function generates SQL DROP ROLE statements for all non-system roles in a PostgreSQL database, used by pg_dumpall to create scripts that clean up existing roles before restoration.

## Definition

```c
static void
dropRoles(PGconn *conn)
```
## Detailed Description
The dropRoles function is part of PostgreSQL's pg_dumpall utility and generates SQL statements to drop existing database roles. It queries the system catalogs to retrieve all role names (excluding PostgreSQL system roles starting with 'pg_' for server versions 9.6 and later), then outputs DROP ROLE statements for each found role.

The function handles different PostgreSQL server versions: for version 9.6 and later, it excludes system roles using a regular expression filter, while earlier versions include all roles. The generated DROP statements can optionally include "IF EXISTS" clauses based on the global  flag, making the script more robust when roles may not exist during restoration.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle used to execute queries
## Dependencies
- Functions called/Symbols referenced:
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format SQL query strings)
  - [executeQuery](../e/executeQuery.md) (execute SQL queries against the database)
  - [fmtId](../f/fmtId.md) (format SQL identifiers with proper quoting)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), destroyPQExpBuffer (manage query buffers)
  - [PQfnumber](../P/PQfnumber.md), PQntuples, PQgetvalue, PQclear (PostgreSQL result set handling)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dumpall.c as part of the dump process)

## Notes and Other Information
- Function is marked as , indicating it's only used within pg_dumpall.c
- Uses global variables: , , , and  (output file)
- Handles version differences: PostgreSQL 9.6+ excludes system roles with  filter
- Outputs SQL statements to the global output file pointer 
- The roles are processed in alphabetical order (ORDER BY 1)
- Includes appropriate SQL comments to identify the DROP ROLE section in the output
- Part of pg_dumpall's role management functionality for database cluster restoration