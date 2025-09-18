# exec_parse_message

## Location
src/backend/tcop/postgres.c: 1395 - 1629

## Overview
Executes a "Parse" protocol message, which parses SQL query strings and creates prepared statements for later execution in the PostgreSQL frontend/backend protocol.

## Definition


## Detailed Description
This function implements the Parse phase of PostgreSQL's extended query protocol. It parses the provided SQL query string and creates a cached plan source that can be executed later via Bind and Execute messages. The function handles both named and unnamed prepared statements with different memory management strategies:

- **Named prepared statements**: Parsing occurs in MessageContext and the finished parse trees are copied into the prepared statement's plancache entry
- **Unnamed prepared statements**: Creates a dedicated memory context for parsing to avoid copying overhead since these statements typically have shorter lifespans

The function performs comprehensive validation including transaction state checks, parameter validation, and ensures only single statements are allowed in prepared statements. It integrates with PostgreSQL's monitoring, logging, and statistics systems throughout the parsing process.

## Parameters / Member Variables
- : The SQL query string to be parsed and prepared
- : Name for the prepared statement (empty string for unnamed statements)
- : Array of parameter type OIDs for parameterized queries
- : Number of parameters in the query

## Dependencies
- Functions called/Symbols referenced:
  - pg_parse_query (basic SQL parsing)
  - CreateCachedPlan (creates cached plan source)
  - pg_analyze_and_rewrite_varparams (semantic analysis and query rewriting)
  - CompleteCachedPlan (finalizes the cached plan)
  - StorePreparedStatement (stores named prepared statements)
  - check_log_duration (duration logging)
  - start_xact_command (transaction management)
  - pgstat_report_activity (activity reporting)
- Called from (representative examples):
  - PostgresMain (main message processing loop)

## Notes and Other Information
- Only allows single SQL statements per prepared statement to keep the protocol simple
- Rejects commands in aborted transaction states except for COMMIT/ROLLBACK
- Handles empty query strings as legal input
- Integrates with PostgreSQL's monitoring facilities including debug logging, statistics collection, and process display updates
- Sends ParseComplete message to client upon successful completion
- Memory management differs significantly between named and unnamed prepared statements for performance optimization