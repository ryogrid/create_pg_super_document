# create_subscription

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1691 - 1748

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
  - createPQExpBuffer
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
  - destroyPQExpBuffer
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