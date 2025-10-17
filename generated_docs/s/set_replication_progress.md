# set_replication_progress

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1749-1839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1749-L1839)

## Overview
set_replication_progress is a function that configures the initial replication progress for a logical subscription by setting the replication origin to start streaming from a specific LSN position.

## Definition
```c
static void set_replication_progress(PGconn *conn, const struct LogicalRepInfo *dbinfo, const char *lsn)
```

## Detailed Description
This function establishes the initial replication progress for a logical subscription that was created in a disabled state. It first queries the subscription's OID from the PostgreSQL system catalogs, then constructs the appropriate replication origin name following the "pg_%u" format (where %u is the subscription OID). Using the pg_replication_origin_advance() function, it sets the replication progress to the specified LSN, which represents the consistent point where the subscriber was promoted.

The function performs careful validation to ensure exactly one subscription record is found. In dry run mode, it uses invalid values for the subscription OID and LSN for demonstration purposes. The replication origin name format follows the same convention used by PostgreSQL's ApplyWorkerMain() function, ensuring compatibility with the logical replication infrastructure.

## Parameters / Member Variables
- `conn`: Active PostgreSQL database connection used to execute SQL commands
- `dbinfo`: Pointer to LogicalRepInfo structure containing subscription and database information
- `lsn`: String representation of the LSN (Log Sequence Number) to set as the starting replication point

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [PQescapeLiteral](../P/PQescapeLiteral.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - pg_log_info
  - pg_log_debug
  - pg_log_error
  - [disconnect_database](../d/disconnect_database.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [pg_free](../p/pg_free.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - [psprintf](../p/psprintf.md)
  - strtoul
- Called from (representative examples):
  - [setup_subscriber](setup_subscriber.md)

## Notes and Other Information
- Must be called after create_subscription() since it requires the subscription OID
- Uses the "pg_%u" naming convention for replication origins, matching ApplyWorkerMain() expectations
- Performs validation to ensure exactly one subscription record exists
- Supports dry run mode with placeholder invalid values for testing
- The LSN parameter represents the consistent point where logical replication should begin
- Critical for ensuring logical replication starts from the correct position to maintain data consistency
- Uses pg_replication_origin_advance() to set the initial replication progress
- Properly escapes SQL parameters and handles memory management for allocated strings

## Simplified Source

```c
static void set_replication_progress(PGconn *conn, const struct LogicalRepInfo *dbinfo, const char *lsn)
{
    PQExpBuffer str = createPQExpBuffer();
    PGresult *res;
    Oid suboid;
    char *subname, *dbname, *originname, *lsnstr;

    // Escape parameters for SQL safety
    subname = PQescapeLiteral(conn, dbinfo->subname, strlen(dbinfo->subname));
    dbname = PQescapeLiteral(conn, dbinfo->dbname, strlen(dbinfo->dbname));

    // Get subscription OID from system catalogs
    appendPQExpBuffer(str,
                      "SELECT s.oid FROM pg_catalog.pg_subscription s "
                      "INNER JOIN pg_catalog.pg_database d ON (s.subdbid = d.oid) "
                      "WHERE s.subname = %s AND d.datname = %s",
                      subname, dbname);

    res = PQexec(conn, str->data);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("could not obtain subscription OID: %s", PQresultErrorMessage(res));
        disconnect_database(conn, true);
    }

    // Validate exactly one subscription found
    if (PQntuples(res) != 1 && !dry_run) {
        pg_log_error("could not obtain subscription OID: got %d rows, expected %d row",
                    PQntuples(res), 1);
        disconnect_database(conn, true);
    }

    // Handle dry run vs real execution
    if (dry_run) {
        suboid = InvalidOid;
        lsnstr = psprintf("%X/%X", LSN_FORMAT_ARGS((XLogRecPtr) InvalidXLogRecPtr));
    } else {
        suboid = strtoul(PQgetvalue(res, 0, 0), NULL, 10);
        lsnstr = psprintf("%s", lsn);
    }
    PQclear(res);

    // Build replication origin name (format: "pg_%u" where %u is subscription OID)
    originname = psprintf("pg_%u", suboid);

    // Set replication progress using pg_replication_origin_advance()
    pg_log_info("setting the replication progress (node name \"%s\", LSN %s) in database \"%s\"",
                originname, lsnstr, dbinfo->dbname);

    resetPQExpBuffer(str);
    appendPQExpBuffer(str, "SELECT pg_catalog.pg_replication_origin_advance('%s', '%s')",
                      originname, lsnstr);
    pg_log_debug("command is: %s", str->data);

    // Execute the progress setting (unless dry run)
    if (!dry_run) {
        res = PQexec(conn, str->data);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            pg_log_error("could not set replication progress for subscription \"%s\": %s",
                        dbinfo->subname, PQresultErrorMessage(res));
            disconnect_database(conn, true);
        }
        PQclear(res);
    }

    // Cleanup
    PQfreemem(subname);
    PQfreemem(dbname);
    pg_free(originname);
    pg_free(lsnstr);
    destroyPQExpBuffer(str);
}
```