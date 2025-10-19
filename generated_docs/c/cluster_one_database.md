# cluster_one_database

## Location
[src/bin/scripts/clusterdb.c:190-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/clusterdb.c#L190-L227)

## Overview
Performs clustering operation on a single database or a specific table within that database using the PostgreSQL CLUSTER command.

## Definition
```c
static void cluster_one_database(const ConnParams *cparams, const char *table, 
                                const char *progname, bool verbose, bool echo)
```

## Detailed Description
This function connects to a specific PostgreSQL database and executes a CLUSTER command. If a table name is provided, it clusters only that specific table; otherwise, it clusters all tables in the database that have clustered indexes defined. The function constructs the appropriate SQL command, executes it through the maintenance command infrastructure, and handles any errors that occur during execution. It provides detailed error messages that specify whether the failure occurred during table-specific or database-wide clustering.

## Parameters / Member Variables
- `cparams`: Connection parameters structure containing database connection information
- `table`: Name of the specific table to cluster, or NULL to cluster all eligible tables
- `progname`: Program name used for error messages and logging
- `verbose`: If true, adds VERBOSE option to the CLUSTER command for detailed output
- `echo`: If true, echoes the SQL commands being executed

## Dependencies
- Functions called/Symbols referenced:
  - [connectDatabase](connectDatabase.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [appendQualifiedRelation](../a/appendQualifiedRelation.md)
  - [executeMaintenanceCommand](../e/executeMaintenanceCommand.md)
  - [PQdb](../P/PQdb.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQfinish](../P/PQfinish.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - pg_log_error
- Called from:
  - [main](../m/main.md) (in clusterdb.c)
  - [cluster_all_databases](cluster_all_databases.md)

## Notes and Other Information
- Part of the clusterdb utility program
- Uses libpq for database connections and command execution
- Exits with status 1 if clustering fails
- Properly handles both single-table and database-wide clustering scenarios
- Includes qualified relation name handling to support schema-qualified table names

## Simplified Source

```c
static void cluster_one_database(const ConnParams *cparams, const char *table,
                                const char *progname, bool verbose, bool echo)
{
    PQExpBufferData sql;
    PGconn *conn;

    // Connect to the target database
    conn = connectDatabase(cparams, progname, echo, false, true);

    // Build CLUSTER command
    initPQExpBuffer(&sql);
    appendPQExpBufferStr(&sql, "CLUSTER");
    if (verbose)
        appendPQExpBufferStr(&sql, " VERBOSE");
    if (table)
    {
        appendPQExpBufferChar(&sql, ' ');
        appendQualifiedRelation(&sql, table, conn, echo);
    }
    appendPQExpBufferChar(&sql, ';');

    // Execute the CLUSTER command
    if (!executeMaintenanceCommand(conn, sql.data, echo))
    {
        // Report failure and exit
        if (table)
            pg_log_error("clustering of table \"%s\" in database \"%s\" failed: %s",
                        table, PQdb(conn), PQerrorMessage(conn));
        else
            pg_log_error("clustering of database \"%s\" failed: %s",
                        PQdb(conn), PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }

    // Clean up
    PQfinish(conn);
    termPQExpBuffer(&sql);
}
```