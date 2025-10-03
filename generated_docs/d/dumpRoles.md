# dumpRoles

## Location
[src/bin/pg_dump/pg_dumpall.c:787-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L787-L994)

## Overview
The dumpRoles function generates SQL CREATE ROLE and ALTER ROLE statements for all non-system roles in a PostgreSQL database, preserving role properties, passwords, comments, and security labels.

## Definition

```c
static void
dumpRoles(PGconn *conn)
```
## Detailed Description
The dumpRoles function is a core component of PostgreSQL's pg_dumpall utility that extracts role definitions from system catalogs and generates corresponding SQL statements for database cluster restoration. It handles version-specific differences in PostgreSQL's role system, particularly the introduction of the  (Row Level Security bypass) attribute in version 9.5.

The function constructs comprehensive CREATE ROLE and ALTER ROLE statements with all role attributes including superuser status, inheritance rights, database/role creation privileges, login capability, replication rights, connection limits, passwords, validity periods, and comments. For binary upgrades, it preserves the original OIDs to maintain system consistency.

The function processes roles in two phases: first dumping role definitions, then dumping user configuration settings separately to handle potential cross-references between roles.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle used to query system catalogs
## Dependencies
- Functions called/Symbols referenced:
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format SQL query strings for different PostgreSQL versions)
  - [executeQuery](../e/executeQuery.md) (execute SQL queries against the database)
  - atooid (convert string OID to numeric OID type)
  - pg_log_warning (log warning messages for skipped system roles)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md), appendPQExpBuffer, appendPQExpBufferStr (manage query buffer)
  - [fmtId](../f/fmtId.md) (format SQL identifiers with proper quoting)
  - [PQgetisnull](../P/PQgetisnull.md), PQgetvalue (check for NULL values and retrieve result data)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md) (safely append string literals to SQL)
  - [buildShSecLabels](../b/buildShSecLabels.md) (generate security label statements)
  - [dumpUserConfig](dumpUserConfig.md) (dump role-specific configuration parameters)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), destroyPQExpBuffer (manage query buffers)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dumpall.c as part of the cluster dump process)

## Notes and Other Information
- Function is marked as , indicating it's only used within pg_dumpall.c
- Uses global variables: , , , , , , 
- Handles PostgreSQL version differences: 9.6+ excludes system roles, 9.5+ includes , earlier versions set it to false
- Skips roles starting with 'pg_' to avoid system roles, with warning messages
- Uses CREATE ROLE + ALTER ROLE pattern to handle existing roles gracefully
- For binary upgrades, preserves original OIDs except for the current user role
- Dumps role configurations separately after all roles to handle cross-references
- Orders roles alphabetically by name (ORDER BY 2) for consistent output
- Includes comprehensive role attribute handling: SUPERUSER, INHERIT, CREATEROLE, CREATEDB, LOGIN, REPLICATION, BYPASSRLS, CONNECTION LIMIT, PASSWORD, VALID UNTIL