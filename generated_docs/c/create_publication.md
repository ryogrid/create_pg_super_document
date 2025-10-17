# create_publication

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1563-1636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1563-L1636)

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
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md)
  - [PQescapeLiteral](../P/PQescapeLiteral.md)
  - [createPQExpBuffer](createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - pg_log_info
  - pg_log_debug
  - pg_log_error
  - pg_log_error_hint
  - [disconnect_database](../d/disconnect_database.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [setup_publisher](../s/setup_publisher.md)

## Notes and Other Information
- Creates publications with "FOR ALL TABLES" clause to include all tables in the database
- Uses a naming scheme designed to minimize conflicts: "pg_createsubscriber_" + database OID + random number
- Performs existence checking before creation to provide informative error messages
- Supports dry run mode for testing without making actual changes
- Sets the made_publication flag in LogicalRepInfo for proper cleanup handling
- [Publication](../P/Publication.md) names are properly escaped to prevent SQL injection vulnerabilities
- Error handling includes helpful hints for resolving naming conflicts

## Simplified Source

```c
static void create_publication(PGconn *conn, struct LogicalRepInfo *dbinfo)
{
    PQExpBuffer str = createPQExpBuffer();
    PGresult *res;
    char *ipubname_esc, *spubname_esc;

    // Escape publication name for SQL safety
    ipubname_esc = PQescapeIdentifier(conn, dbinfo->pubname, strlen(dbinfo->pubname));
    spubname_esc = PQescapeLiteral(conn, dbinfo->pubname, strlen(dbinfo->pubname));

    // Check if publication already exists
    appendPQExpBuffer(str, "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = %s", spubname_esc);
    res = PQexec(conn, str->data);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("could not obtain publication information: %s", PQresultErrorMessage(res));
        disconnect_database(conn, true);
    }

    // Handle existing publication
    if (PQntuples(res) == 1) {
        pg_log_error("publication \"%s\" already exists", dbinfo->pubname);
        pg_log_error_hint("Consider renaming this publication before continuing.");
        disconnect_database(conn, true);
    }

    PQclear(res);
    resetPQExpBuffer(str);

    // Create the publication for all tables
    pg_log_info("creating publication \"%s\" in database \"%s\"", dbinfo->pubname, dbinfo->dbname);
    appendPQExpBuffer(str, "CREATE PUBLICATION %s FOR ALL TABLES", ipubname_esc);
    pg_log_debug("command is: %s", str->data);

    // Execute creation (unless dry run)
    if (!dry_run) {
        res = PQexec(conn, str->data);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            pg_log_error("could not create publication \"%s\" in database \"%s\": %s",
                        dbinfo->pubname, dbinfo->dbname, PQresultErrorMessage(res));
            disconnect_database(conn, true);
        }
        PQclear(res);
    }

    // Mark as created for cleanup tracking
    dbinfo->made_publication = true;

    // Cleanup
    PQfreemem(ipubname_esc);
    PQfreemem(spubname_esc);
    destroyPQExpBuffer(str);
}
```