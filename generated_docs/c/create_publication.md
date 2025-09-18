# create_publication

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1563 - 1636

## Overview
create_publication is a function that creates a PostgreSQL publication including all tables in a specified database, primarily used in the pg_createsubscriber utility for setting up logical replication.

## Definition
```c
static void create_publication(PGconn *conn, struct LogicalRepInfo *dbinfo)
```

## Detailed Description
This function creates a PostgreSQL publication that includes all tables in the target database. It first checks whether a publication with the specified name already exists to avoid conflicts. If the publication already exists, it logs an error and terminates the process with a helpful hint to rename the existing publication. The function generates a unique publication name with the "pg_createsubscriber_" prefix followed by the database OID and a random number to minimize naming conflicts.

The function uses proper SQL escaping for both identifiers and literals when constructing queries. It respects dry run mode by skipping the actual CREATE PUBLICATION command execution while still performing all validation steps. After successful creation, it marks the publication as created in the LogicalRepInfo structure for cleanup tracking purposes.

## Parameters / Member Variables
- `conn`: Active PostgreSQL database connection used to execute SQL commands
- `dbinfo`: Pointer to LogicalRepInfo structure containing database information including publication name and database name

## Dependencies
- Functions called/Symbols referenced:
  - PQescapeIdentifier
  - PQescapeLiteral
  - createPQExpBuffer
  - appendPQExpBuffer
  - PQexec
  - PQresultStatus
  - PQresultErrorMessage
  - PQntuples
  - PQclear
  - resetPQExpBuffer
  - pg_log_info
  - pg_log_debug
  - pg_log_error
  - pg_log_error_hint
  - disconnect_database
  - PQfreemem
  - destroyPQExpBuffer
- Called from (representative examples):
  - setup_publisher

## Notes and Other Information
- Creates publications with "FOR ALL TABLES" clause to include all tables in the database
- Uses a naming scheme designed to minimize conflicts: "pg_createsubscriber_" + database OID + random number
- Performs existence checking before creation to provide informative error messages
- Supports dry run mode for testing without making actual changes
- Sets the made_publication flag in LogicalRepInfo for proper cleanup handling
- Publication names are properly escaped to prevent SQL injection vulnerabilities
- Error handling includes helpful hints for resolving naming conflicts