# create_subscription

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1691-1748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1691-L1748)

## Overview
create_subscription is a function that creates a PostgreSQL logical replication subscription with predefined options, designed to work with existing replication slots and publications created in previous steps of the pg_createsubscriber process.

## Definition
```c
static void create_subscription(PGconn *conn, const struct LogicalRepInfo *dbinfo)
```

## Detailed Description
This function creates a logical replication subscription that connects to a publisher database using an existing replication slot. The subscription is created in a disabled state (enabled = false) because the replication progress needs to be set before activation. The function uses several predefined options: create_slot is set to false since the replication slot already exists, copy_data is disabled to avoid initial data copying, and it references the existing replication slot by name.

The function constructs a CREATE SUBSCRIPTION SQL command with proper escaping for all parameters including publication name, subscription name, connection information, and replication slot name. It's designed to work as part of a multi-step process where the replication slot is created beforehand and the replication progress will be configured afterward via set_replication_progress().

## Parameters / Member Variables
- `conn`: Active PostgreSQL database connection used to execute the CREATE SUBSCRIPTION command
- `dbinfo`: Pointer to LogicalRepInfo structure containing all subscription details including names, connection info, and replication slot information

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](createPQExpBuffer.md)
  - [PQescapeIdentifier](../P/PQescapeIdentifier.md)
  - [PQescapeLiteral](../P/PQescapeLiteral.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - pg_log_info
  - pg_log_debug
  - pg_log_error
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [setup_subscriber](../s/setup_subscriber.md)

## Notes and Other Information
- Creates subscription in disabled state to allow replication progress setup first
- Uses existing replication slot (create_slot = false) created in previous steps
- Disables initial data copying (copy_data = false) as data is already synchronized
- Requires subsequent call to set_replication_progress() to configure replication origin
- The replication origin name includes the subscription OID, which is only available after subscription creation
- Properly escapes all SQL parameters to prevent injection attacks
- Supports dry run mode for testing without making actual changes
- Part of the multi-step pg_createsubscriber process for converting physical replicas to logical subscriptions

## Simplified Source

```c
static void create_subscription(PGconn *conn, const struct LogicalRepInfo *dbinfo)
{
    PQExpBuffer str = createPQExpBuffer();
    PGresult *res;
    char *pubname_esc, *subname_esc, *pubconninfo_esc, *replslotname_esc;

    // Escape all parameters for SQL safety
    pubname_esc = PQescapeIdentifier(conn, dbinfo->pubname, strlen(dbinfo->pubname));
    subname_esc = PQescapeIdentifier(conn, dbinfo->subname, strlen(dbinfo->subname));
    pubconninfo_esc = PQescapeLiteral(conn, dbinfo->pubconninfo, strlen(dbinfo->pubconninfo));
    replslotname_esc = PQescapeLiteral(conn, dbinfo->replslotname, strlen(dbinfo->replslotname));

    // Create subscription command with predefined options
    pg_log_info("creating subscription \"%s\" in database \"%s\"", dbinfo->subname, dbinfo->dbname);

    appendPQExpBuffer(str,
                      "CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s "
                      "WITH (create_slot = false, enabled = false, "
                      "slot_name = %s, copy_data = false)",
                      subname_esc, pubconninfo_esc, pubname_esc, replslotname_esc);

    pg_log_debug("command is: %s", str->data);

    // Execute the subscription creation (unless dry run)
    if (!dry_run) {
        res = PQexec(conn, str->data);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            pg_log_error("could not create subscription \"%s\" in database \"%s\": %s",
                        dbinfo->subname, dbinfo->dbname, PQresultErrorMessage(res));
            disconnect_database(conn, true);
        }
        PQclear(res);
    }

    // Cleanup escaped strings
    PQfreemem(pubname_esc);
    PQfreemem(subname_esc);
    PQfreemem(pubconninfo_esc);
    PQfreemem(replslotname_esc);
    destroyPQExpBuffer(str);
}
```