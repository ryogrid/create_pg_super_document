# reindex_all_databases

## Location
[src/bin/scripts/reindexdb.c:820-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/reindexdb.c#L820-L883)

## Overview
Reindexes all databases in a PostgreSQL cluster that allow connections, iterating through each database and applying the specified reindexing operations.

## Definition

```c
static void
reindex_all_databases(ConnParams *cparams,
					  const char *progname, bool echo, bool quiet, bool verbose,
					  bool concurrently, int concurrentCons,
					  const char *tablespace, bool syscatalog,
					  SimpleStringList *schemas, SimpleStringList *tables,
					  SimpleStringList *indexes)
```
## Detailed Description
This function is part of the  command-line utility and handles the "--all" option to reindex all databases in a PostgreSQL cluster. It first connects to a maintenance database to query the system catalog for a list of all databases that allow connections (excluding template databases with datconnlimit = -2). For each database found, it performs the requested reindexing operations in a specific order: system catalogs first (if requested), then schemas, indexes, tables, and finally the entire database (if no specific objects were specified).

The function respects the hierarchy of reindexing operations and ensures that more specific operations (indexes, tables, schemas) take precedence over general database-wide reindexing. It also handles concurrent reindexing options and can optionally move indexes to a different tablespace during the reindex operation.

## Parameters / Member Variables
- `*cparams`: Connection parameters structure containing database connection information
- `*progname`: Name of the program (typically "reindexdb") for error messages and output
- `echo`: If true, echo the SQL commands being executed to stdout
- `quiet`: If true, suppress informational output messages
- `verbose`: If true, provide detailed progress information
- `concurrently`: If true, perform reindexing concurrently (non-blocking)
- `concurrentCons`: Number of concurrent connections to use for parallel operations
- `*tablespace`: Optional tablespace name to move indexes to during reindexing
- `syscatalog`: If true, reindex system catalogs
- `*schemas`: List of specific schemas to reindex
- `*tables`: List of specific tables to reindex
- `*indexes`: List of specific indexes to reindex
## Dependencies
- Functions called/Symbols referenced:
  - [connectMaintenanceDatabase](../c/connectMaintenanceDatabase.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQfinish](../P/PQfinish.md)
  - [reindex_one_database](reindex_one_database.md)
  - REINDEX_SYSTEM
  - REINDEX_SCHEMA
  - REINDEX_INDEX
  - REINDEX_TABLE
  - REINDEX_DATABASE
- Called from (representative examples):
  - [main](../m/main.md) (in reindexdb.c)

## Notes and Other Information
- This is a static function internal to the reindexdb utility
- The function queries pg_database to find all databases where datallowconn=true and datconnlimit≠-2
- Reindexing operations are performed in a specific priority order: system catalogs, schemas, indexes, tables, then full database
- Full database reindexing only occurs if no specific objects (schemas, tables, indexes) are specified and syscatalog is false
- The function temporarily overrides the database name in cparams for each database being processed
- Uses different concurrency settings for different operations (system catalogs and indexes use concurrency=1)

## Simplified Source

```c
static void
reindex_all_databases(ConnParams *cparams,
                      const char *progname, bool echo, bool quiet, bool verbose,
                      bool concurrently, int concurrentCons,
                      const char *tablespace, bool syscatalog,
                      SimpleStringList *schemas, SimpleStringList *tables,
                      SimpleStringList *indexes)
{
    PGconn *conn;
    PGresult *result;
    int i;

    // Connect to maintenance database and get list of all connectable databases
    conn = connectMaintenanceDatabase(cparams, progname, echo);
    result = executeQuery(conn,
                         "SELECT datname FROM pg_database "
                         "WHERE datallowconn AND datconnlimit <> -2 "
                         "ORDER BY 1;",
                         echo);
    PQfinish(conn);

    // Process each database found
    for (i = 0; i < PQntuples(result); i++) {
        char *dbname = PQgetvalue(result, i, 0);

        // Show progress message
        if (!quiet) {
            printf("%s: reindexing database \"%s\"\n", progname, dbname);
            fflush(stdout);
        }

        // Override database name for this iteration
        cparams->override_dbname = dbname;

        // Perform reindexing operations in priority order:

        // 1. System catalogs (if requested, single connection)
        if (syscatalog)
            reindex_one_database(cparams, REINDEX_SYSTEM, NULL,
                                progname, echo, verbose,
                                concurrently, 1, tablespace);

        // 2. Specific schemas (if specified)
        if (schemas->head != NULL)
            reindex_one_database(cparams, REINDEX_SCHEMA, schemas,
                                progname, echo, verbose,
                                concurrently, concurrentCons, tablespace);

        // 3. Specific indexes (if specified, single connection)
        if (indexes->head != NULL)
            reindex_one_database(cparams, REINDEX_INDEX, indexes,
                                progname, echo, verbose,
                                concurrently, 1, tablespace);

        // 4. Specific tables (if specified)
        if (tables->head != NULL)
            reindex_one_database(cparams, REINDEX_TABLE, tables,
                                progname, echo, verbose,
                                concurrently, concurrentCons, tablespace);

        // 5. Entire database (only if no specific objects specified)
        if (!syscatalog && indexes->head == NULL &&
            tables->head == NULL && schemas->head == NULL)
            reindex_one_database(cparams, REINDEX_DATABASE, NULL,
                                progname, echo, verbose,
                                concurrently, concurrentCons, tablespace);
    }

    PQclear(result);
}
```