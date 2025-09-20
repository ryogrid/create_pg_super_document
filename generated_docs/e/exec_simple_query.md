# exec_simple_query

## Location
[src/backend/tcop/postgres.c:1017-1394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L1017-L1394)

## Overview
Executes a "simple Query" protocol message, handling the complete SQL query processing pipeline from parsing through execution for PostgreSQL's simple query protocol.

## Definition

```c
structing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);
```
## Detailed Description
This function is the main entry point for PostgreSQL's simple query protocol, implementing the complete query processing pipeline from raw SQL text to result delivery. It handles multiple SQL statements within a single query string and manages transaction boundaries appropriately.

The function operates through several key phases:

1. **Initialization and Monitoring**: Sets up query monitoring, activity reporting, and performance statistics collection.

2. **Transaction Management**: Starts transaction commands and manages implicit transaction blocks for multi-statement queries.

3. **Parse Processing**: Converts the raw SQL string into parse trees using .

4. **Statement Processing Loop**: For each parsed statement:
   - **Command Analysis**: Creates command tags and handles process status display
   - **Transaction State Validation**: Ensures queries can execute in current transaction state
   - **Snapshot Management**: Sets up appropriate snapshots for analysis and planning
   - **Query Processing**: Performs analysis, rewriting, and planning via  and 
   - **Portal Creation**: Creates an unnamed portal for query execution
   - **Execution**: Runs the query through  with appropriate output formatting
   - **Cleanup**: Manages memory contexts and transaction boundaries

5. **Transaction Finalization**: Handles implicit transaction blocks and transaction command completion.

6. **Performance Reporting**: Logs duration and performance statistics as configured.

The function includes extensive error handling, memory management through dedicated contexts, and comprehensive monitoring integration.

## Parameters / Member Variables
- : Raw SQL query string to be executed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_query](../p/pg_parse_query.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - [pg_plan_queries](../p/pg_plan_queries.md)
  - [start_xact_command](../s/start_xact_command.md)
  - [finish_xact_command](../f/finish_xact_command.md)
  - CreatePortal
  - [PortalDefineQuery](../P/PortalDefineQuery.md)
  - [PortalStart](../P/PortalStart.md)
  - [PortalRun](../P/PortalRun.md)
  - PortalDrop
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [CreateCommandTag](../C/CreateCommandTag.md)
  - [BeginCommand](../B/BeginCommand.md)
  - [EndCommand](../E/EndCommand.md)
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md)
  - [BeginImplicitTransactionBlock](../B/BeginImplicitTransactionBlock.md)
  - [EndImplicitTransactionBlock](../E/EndImplicitTransactionBlock.md)
  - Many memory management and utility functions
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (multiple call sites)

## Notes and Other Information
- Core function in PostgreSQL's simple query protocol implementation
- Manages complex transaction semantics for multi-statement queries using implicit transaction blocks
- Includes comprehensive performance monitoring and debugging support
- Handles both regular DML/DDL statements and utility commands
- Located in src/backend/tcop/postgres.c:1017-1394
- Implements proper memory context management to prevent memory leaks
- Supports extensive logging and tracing capabilities through various PostgreSQL subsystems
- Critical for PostgreSQL's compliance with the PostgreSQL wire protocol
- The function ensures that COMMIT/ROLLBACK statements properly separate transaction boundaries even within multi-statement queries
- Includes special handling for binary cursor operations and output formatting