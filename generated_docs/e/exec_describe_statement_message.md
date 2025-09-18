# exec_describe_statement_message

## Location
src/backend/tcop/postgres.c: 2625 - 2717

## Overview
Processes a "Describe" message for a prepared statement, sending parameter and result set descriptions back to the client in the PostgreSQL wire protocol.

## Definition


## Detailed Description
This function handles the Describe message for prepared statements in PostgreSQL's wire protocol. It retrieves information about a prepared statement (either named or unnamed) and sends back two types of descriptions to the client: parameter descriptions and row descriptions. The function starts a transaction command to ensure proper transaction context, then locates the prepared statement and validates that it can be described safely. If the transaction is in an aborted state, it restricts descriptions of statements that return data to avoid catalog access issues.

The function sends parameter type information for all statement parameters and either a row description (for statements that return data) or a NoData message (for statements that don't return data).

## Parameters / Member Variables
- `stmt_name`: Name of the prepared statement to describe. If empty string, refers to the unnamed prepared statement.

## Dependencies
- Functions called/Symbols referenced:
  - start_xact_command
  - FetchPreparedStatement 
  - IsAbortedTransactionBlockState
  - CachedPlanGetTargetList
  - SendRowDescriptionMessage
  - pq_beginmessage_reuse
  - pq_sendint16
  - pq_sendint32
  - pq_endmessage_reuse
  - pq_putemptymessage
- Called from (representative examples):
  - PostgresMain

## Notes and Other Information
- The function handles both named and unnamed prepared statements
- Special safety checks prevent describing result-returning statements in aborted transaction states
- Uses reusable message buffers for efficient wire protocol communication
- Prepared statements are expected to have fixed result descriptors
- Part of PostgreSQL's extended query protocol implementation