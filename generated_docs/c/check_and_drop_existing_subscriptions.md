# check_and_drop_existing_subscriptions

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1103-1142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1103-L1142)

## Overview
Retrieves and drops all pre-existing subscriptions for a specified database in PostgreSQL's logical replication setup.

## Definition

```c
static void
check_and_drop_existing_subscriptions(PGconn *conn,
									  const struct LogicalRepInfo *dbinfo)
```
## Detailed Description
This function is part of the pg_createsubscriber utility that converts a standby server into a logical replica. It performs cleanup by identifying and removing any existing subscriptions in the target database. The function queries the pg_subscription catalog to find subscriptions associated with the specified database, then calls drop_existing_subscriptions() to remove each one. This ensures a clean state before setting up new logical replication subscriptions.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle used to execute queries
- `*dbinfo`: Pointer to LogicalRepInfo structure containing database information, specifically the database name to check for subscriptions
## Dependencies
- Functions called/Symbols referenced:
  - [PQescapeLiteral](../P/PQescapeLiteral.md) (escapes database name for safe SQL usage)
  - [PQexec](../P/PQexec.md) (executes the subscription query)
  - [PQresultStatus](../P/PQresultStatus.md)/PGRES_TUPLES_OK (checks query result status)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (retrieves error messages on failure)
  - [disconnect_database](../d/disconnect_database.md) (handles connection cleanup on error)
  - [drop_existing_subscriptions](../d/drop_existing_subscriptions.md) (removes individual subscriptions)
  - [PQfreemem](../P/PQfreemem.md) (frees escaped string memory)
- Called from:
  - [setup_subscriber](../s/setup_subscriber.md) (main subscription setup function)

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- Performs proper error handling and resource cleanup
- Uses parameterized queries with escaped literals for SQL injection prevention
- Part of the logical replication infrastructure for converting standby to subscriber

## Simplified Source

```c
static void
check_and_drop_existing_subscriptions(PGconn *conn,
                                       const struct LogicalRepInfo *dbinfo)
{
    PQExpBuffer query = createPQExpBuffer();
    char *dbname;
    PGresult *res;

    Assert(conn != NULL);

    // Escape database name for safe SQL usage
    dbname = PQescapeLiteral(conn, dbinfo->dbname, strlen(dbinfo->dbname));

    // Query for existing subscriptions in the specified database
    appendPQExpBuffer(query,
                      "SELECT s.subname FROM pg_catalog.pg_subscription s "
                      "INNER JOIN pg_catalog.pg_database d ON (s.subdbid = d.oid) "
                      "WHERE d.datname = %s",
                      dbname);

    res = PQexec(conn, query->data);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("could not obtain pre-existing subscriptions: %s",
                     PQresultErrorMessage(res));
        disconnect_database(conn, true);
    }

    // Drop each existing subscription found
    for (int i = 0; i < PQntuples(res); i++)
        drop_existing_subscriptions(conn, PQgetvalue(res, i, 0), dbinfo->dbname);

    // Clean up resources
    PQclear(res);
    destroyPQExpBuffer(query);
    PQfreemem(dbname);
}
```