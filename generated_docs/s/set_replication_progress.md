# set_replication_progress

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1749 - 1839

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
  - createPQExpBuffer
  - PQescapeLiteral
  - appendPQExpBuffer
  - PQexec
  - PQresultStatus
  - PQresultErrorMessage
  - PQntuples
  - PQgetvalue
  - PQclear
  - resetPQExpBuffer
  - pg_log_info
  - pg_log_debug
  - pg_log_error
  - disconnect_database
  - PQfreemem
  - pg_free
  - destroyPQExpBuffer
  - psprintf
  - strtoul
- Called from (representative examples):
  - setup_subscriber

## Notes and Other Information
- Must be called after create_subscription() since it requires the subscription OID
- Uses the "pg_%u" naming convention for replication origins, matching ApplyWorkerMain() expectations
- Performs validation to ensure exactly one subscription record exists
- Supports dry run mode with placeholder invalid values for testing
- The LSN parameter represents the consistent point where logical replication should begin
- Critical for ensuring logical replication starts from the correct position to maintain data consistency
- Uses pg_replication_origin_advance() to set the initial replication progress
- Properly escapes SQL parameters and handles memory management for allocated strings