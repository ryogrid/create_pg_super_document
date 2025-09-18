# drop_existing_subscriptions

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1062 - 1102

## Overview
Drops a specified subscription to avoid duplicate subscriptions when converting a standby server to a subscriber, preserving the associated replication slot for publisher use.

## Definition


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
  - createPQExpBuffer
  - appendPQExpBuffer
  - pg_log_info
  - PQexec
  - PGRES_COMMAND_OK
  - PQresultErrorMessage
  - disconnect_database
  - PQclear
  - destroyPQExpBuffer
- Called from (representative examples):
  - check_and_drop_existing_subscriptions

## Notes and Other Information
- Commands are executed within a single transaction for atomicity
- Respects dry_run mode by skipping actual execution while still logging the intended action
- Preserves replication slots by setting slot_name to NONE before dropping the subscription
- Essential for preventing subscription conflicts during standby-to-subscriber conversion
- Terminates the program if the drop operation fails