# pg_parse_query

## Location
src/backend/tcop/postgres.c: 615 - 674

## Overview
Performs raw parsing of SQL query strings, returning a list of parse trees (RawStmt nodes) without performing analysis or rewriting.

## Definition
```c
List *pg_parse_query(const char *query_string)
```

## Detailed Description
pg_parse_query is a fundamental function in PostgreSQL's query processing pipeline that performs only the raw parsing stage of SQL query processing. It takes a SQL query string and converts it into a list of RawStmt nodes representing the parse tree(s) for the query. The function is deliberately separated from analysis and rewriting stages because those operations require access to database tables and cannot be performed during aborted transactions.

The function includes optional debugging capabilities controlled by compile-time flags: COPY_PARSE_PLAN_TREES tests the copyObject() and equal() functions, while WRITE_READ_PARSE_PLAN_TREES tests the outfuncs/readfuncs serialization mechanisms. Performance statistics can be logged if log_parser_stats is enabled, and tracing is supported through TRACE_POSTGRESQL_QUERY_PARSE_START/DONE macros.

This separation is crucial for PostgreSQL's transaction handling, as it allows the system to parse commands like COMMIT or ABORT even when in an aborted transaction state, where database access for analysis would fail.

## Parameters / Member Variables
- `query_string`: The SQL query string to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - raw_parser
  - RAW_PARSE_DEFAULT
  - ResetUsage
  - ShowUsage
  - copyObject (debugging)
  - equal (debugging)
  - nodeToStringWithLocations (debugging)
  - stringToNodeWithLocations (debugging)
- Called from (representative examples):
  - exec_simple_query
  - exec_parse_message
  - fmgr_sql_validator
  - init_sql_fcache
  - inline_function

## Notes and Other Information
- Returns a List of RawStmt nodes since multiple commands may be present in a single query string
- Critical for transaction state management - can parse COMMIT/ABORT commands in aborted transactions
- Includes comprehensive debugging support for development and testing
- Performance monitoring available through log_parser_stats configuration
- Part of PostgreSQL's modular query processing architecture that separates parsing from semantic analysis
- Essential for maintaining system stability during error conditions and transaction failures