# pg_parse_query

## Location
[src/backend/tcop/postgres.c:615-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L615-L674)

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
  - [raw_parser](../r/raw_parser.md)
  - RAW_PARSE_DEFAULT
  - ResetUsage
  - ShowUsage
  - copyObject (debugging)
  - [equal](../e/equal.md) (debugging)
  - [nodeToStringWithLocations](../n/nodeToStringWithLocations.md) (debugging)
  - [stringToNodeWithLocations](../s/stringToNodeWithLocations.md) (debugging)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_parse_message](../e/exec_parse_message.md)
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [inline_function](../i/inline_function.md)

## Notes and Other Information
- Returns a List of RawStmt nodes since multiple commands may be present in a single query string
- Critical for transaction state management - can parse COMMIT/ABORT commands in aborted transactions
- Includes comprehensive debugging support for development and testing
- Performance monitoring available through log_parser_stats configuration
- Part of PostgreSQL's modular query processing architecture that separates parsing from semantic analysis
- Essential for maintaining system stability during error conditions and transaction failures