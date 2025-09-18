# enable_subscription

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1840 - 1874

## Overview
The enable_subscription function activates a previously created but disabled subscription in PostgreSQL logical replication.

## Definition


## Detailed Description
This function is part of the pg_createsubscriber utility and is responsible for enabling a logical replication subscription that was created in a disabled state during an earlier step of the subscription setup process. The function executes an ALTER SUBSCRIPTION ENABLE command to activate the subscription after the initial logical replication location has been properly adjusted. It includes comprehensive error handling and logging to track the operation's progress and handle potential failures during the enable operation.

## Parameters / Member Variables
- : PGconn pointer representing the database connection to execute the enable command
- : Pointer to LogicalRepInfo struct containing subscription details including subscription name and database name

## Dependencies
- Functions called/Symbols referenced:
  - PQescapeIdentifier (escapes subscription name for SQL safety)
  - pg_log_info (logs informational messages)
  - pg_log_debug (logs debug-level command information)
  - PQexec (executes the ALTER SUBSCRIPTION command)
  - PQresultStatus (checks command execution result)
  - PQresultErrorMessage (retrieves error messages on failure)
  - disconnect_database (handles database disconnection on errors)
  - PQfreemem (frees escaped identifier memory)
  - createPQExpBuffer/destroyPQExpBuffer (manages query buffer)
- Called from (representative examples):
  - setup_subscriber (main subscription setup workflow)

## Notes and Other Information
- The function is marked as static, indicating it's only used within the pg_createsubscriber.c file
- Includes dry-run support through the global dry_run variable - when enabled, the command is logged but not executed
- Uses proper SQL identifier escaping to prevent SQL injection attacks
- Implements robust error handling that terminates the database connection on command failure
- Part of the larger pg_createsubscriber utility workflow for converting a physical replica to a logical subscriber