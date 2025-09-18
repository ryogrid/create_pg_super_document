# check_log_statement

## Location
src/backend/tcop/postgres.c: 2368 - 2406

## Overview
Determines whether SQL commands should be logged based on the log_statement configuration setting by examining statement types.

## Definition


## Detailed Description
This function implements PostgreSQL's statement logging policy by evaluating whether statements in the provided list should be logged according to the current log_statement configuration. The function supports PostgreSQL's hierarchical logging levels:

- : No statements are logged
- : Only DDL (Data Definition Language) statements are logged  
- : DDL and DML (Data Modification Language) statements are logged
- : All statements including SELECT queries are logged

The function handles both raw parse trees from the grammar and planned statement lists, making it versatile for use across different execution phases. It evaluates each statement in the list and returns true if any statement meets the logging criteria.

## Parameters / Member Variables
- : List of SQL statements to evaluate, can contain either raw parse tree nodes or planned statement nodes

## Dependencies
- Functions called/Symbols referenced:
  - GetCommandLogLevel (determines the logging level required for a specific statement type)
  - LOGSTMT_NONE/LOGSTMT_ALL (logging level constants)
- Called from (representative examples):
  - exec_simple_query (simple query execution path)
  - exec_execute_message (extended query protocol execution)

## Notes and Other Information
- Implements short-circuit evaluation: returns immediately when log_statement is LOGSTMT_NONE (false) or LOGSTMT_ALL (true)
- Uses GetCommandLogLevel to determine the minimum log level required for each statement type
- Supports mixed statement lists by checking each statement individually
- Critical for PostgreSQL's security and auditing capabilities by controlling which SQL operations are recorded in logs
- Part of PostgreSQL's comprehensive logging infrastructure that helps database administrators monitor and audit database activity