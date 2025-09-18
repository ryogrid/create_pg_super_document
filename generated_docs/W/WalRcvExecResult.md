# WalRcvExecResult

## Location
src/include/replication/walreceiver.h: 217 - 224

## Overview
The WalRcvExecResult structure is a return value container used by the PostgreSQL WAL (Write-Ahead Log) receiver to encapsulate the results of executing queries through the walrcv_exec function, including execution status and any returned tuples.

## Definition


## Detailed Description
WalRcvExecResult is a composite data structure designed to hold comprehensive information about the execution of SQL commands or replication protocol commands through the WAL receiver interface. This structure enables the WAL receiver subsystem to communicate detailed execution results back to calling functions, including success/failure status, error information, and any result data.

The structure is primarily used in PostgreSQL's logical replication and subscription management systems, where the WAL receiver needs to execute queries on remote nodes and return both the execution status and any resulting data to the caller. This allows for robust error handling and data processing in distributed PostgreSQL environments.

## Parameters / Member Variables
- : A WalRcvExecStatus enum value indicating the outcome of query execution (success with different result types, or error)
- : An integer representing the SQL state code when an error occurs during query execution
- : A character pointer to an error message string providing detailed error information when execution fails
- : A pointer to a Tuplestorestate structure containing the result tuples when the query returns data
- : A TupleDesc structure describing the format and metadata of the returned tuples

## Dependencies
- Functions called/Symbols referenced:
  - WalRcvExecStatus
  - Tuplestorestate
  - [TupleDesc](../T/TupleDesc.md)

- Called from (representative examples):
  - [check_publications](../c/check_publications.md) (src/backend/commands/subscriptioncmds.c:488)
  - [ReplicationSlotDropAtPubNode](../R/ReplicationSlotDropAtPubNode.md) (src/backend/commands/subscriptioncmds.c:1857)
  - [libpqrcv_exec](../l/libpqrcv_exec.md) (src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1239)
  - [fetch_remote_table_info](../f/fetch_remote_table_info.md) (src/backend/replication/logical/tablesync.c:823)
  - [copy_table](../c/copy_table.md) (src/backend/replication/logical/tablesync.c:1146)

## Notes and Other Information
- The structure is typically used in conjunction with walrcv_clear_result() for proper memory management and cleanup
- The sqlstate and err fields are primarily populated when status indicates WALRCV_ERROR
- The tuplestore and tupledesc fields are relevant when status indicates WALRCV_OK_TUPLES
- This structure is part of PostgreSQL's replication infrastructure and is essential for logical replication, subscription management, and slot synchronization operations
- The structure enables type-safe handling of different query result types through the status field enumeration