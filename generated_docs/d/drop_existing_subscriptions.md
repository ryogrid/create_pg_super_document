# drop_existing_subscriptions

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1062-1102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1062-L1102)

## Overview
Drops a specified subscription to avoid duplicate subscriptions when converting a standby server to a subscriber, preserving the associated replication slot for publisher use.

## Definition

```c
struct a query string. These commands are allowed to be executed
	 * within a transaction.
	 */
	appendPQExpBuffer(query, "ALTER SUBSCRIPTION %s DISABLE;
```
## Detailed Description
This function safely removes an existing subscription by executing a sequence of SQL commands within a transaction. The process follows a specific order to avoid conflicts:

1. Disables the subscription to stop active replication
2. Detaches the replication slot by setting slot_name to NONE (preserving the slot for publisher use)
3. Drops the subscription object

The function is designed to handle the scenario where a standby server being converted to a subscriber already has subscriptions that would conflict with the new logical replication setup. It preserves replication slots because they may still be needed by the publisher.

## Parameters / Member Variables
- : PostgreSQL database connection handle for executing the drop commands
- : Name of the subscription to be dropped
- : Name of the database containing the subscription (used for logging only)

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - pg_log_info
  - [PQexec](../P/PQexec.md)
  - PGRES_COMMAND_OK
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [disconnect_database](disconnect_database.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - [check_and_drop_existing_subscriptions](../c/check_and_drop_existing_subscriptions.md)

## Notes and Other Information
- Commands are executed within a single transaction for atomicity
- Respects dry_run mode by skipping actual execution while still logging the intended action
- Preserves replication slots by setting slot_name to NONE before dropping the subscription
- Essential for preventing subscription conflicts during standby-to-subscriber conversion
- Terminates the program if the drop operation fails

## Simplified Source

```c
static void
drop_existing_subscriptions(PGconn *conn, const char *subname, const char *dbname)
{
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;

    Assert(conn != NULL);

    // Build command sequence to safely drop subscription
    // 1. Disable subscription to stop replication
    appendPQExpBuffer(query, "ALTER SUBSCRIPTION %s DISABLE;", subname);

    // 2. Detach slot to preserve it for publisher use
    appendPQExpBuffer(query, " ALTER SUBSCRIPTION %s SET (slot_name = NONE);", subname);

    // 3. Drop the subscription object
    appendPQExpBuffer(query, " DROP SUBSCRIPTION %s;", subname);

    pg_log_info("dropping subscription \"%s\" in database \"%s\"", subname, dbname);

    // Execute commands unless in dry-run mode
    if (!dry_run) {
        res = PQexec(conn, query->data);

        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            pg_log_error("could not drop subscription \"%s\": %s",
                         subname, PQresultErrorMessage(res));
            disconnect_database(conn, true);
        }

        PQclear(res);
    }

    destroyPQExpBuffer(query);
}
```