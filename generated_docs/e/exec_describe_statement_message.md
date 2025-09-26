# exec_describe_statement_message

## Location
[src/backend/tcop/postgres.c:2625-2717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2625-L2717)

## Overview
Processes a "Describe" message for a prepared statement, sending parameter and result set descriptions back to the client in the PostgreSQL wire protocol.

## Definition

```c
static void
exec_describe_statement_message(const char *stmt_name)
```
## Detailed Description
This function handles the Describe message for prepared statements in PostgreSQL's wire protocol. It retrieves information about a prepared statement (either named or unnamed) and sends back two types of descriptions to the client: parameter descriptions and row descriptions. The function starts a transaction command to ensure proper transaction context, then locates the prepared statement and validates that it can be described safely. If the transaction is in an aborted state, it restricts descriptions of statements that return data to avoid catalog access issues.

The function sends parameter type information for all statement parameters and either a row description (for statements that return data) or a NoData message (for statements that don't return data).

## Parameters / Member Variables
- `stmt_name`: Name of the prepared statement to describe. If empty string, refers to the unnamed prepared statement.

## Dependencies
- Functions called/Symbols referenced:
  - [start_xact_command](../s/start_xact_command.md)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) 
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md)
  - [CachedPlanGetTargetList](../C/CachedPlanGetTargetList.md)
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [pq_beginmessage_reuse](../p/pq_beginmessage_reuse.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_endmessage_reuse](../p/pq_endmessage_reuse.md)
  - [pq_putemptymessage](../p/pq_putemptymessage.md)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- The function handles both named and unnamed prepared statements
- Special safety checks prevent describing result-returning statements in aborted transaction states
- Uses reusable message buffers for efficient wire protocol communication
- Prepared statements are expected to have fixed result descriptors
- Part of PostgreSQL's extended query protocol implementation