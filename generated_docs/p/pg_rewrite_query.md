# pg_rewrite_query

## Location
src/backend/tcop/postgres.c: 808 - 889

## Overview
Performs query rewriting on a Query structure produced by parse analysis, transforming queries to handle views, rules, and other PostgreSQL rewrite mechanisms.

## Definition


## Detailed Description
This function is responsible for the query rewriting phase of PostgreSQL's query processing pipeline. It takes a Query structure from the parser and applies PostgreSQL's rule system to expand views, apply rules, and perform other query transformations.

The function handles two main cases:
1. **Utility Commands**: For CMD_UTILITY queries (DDL statements like CREATE, DROP, etc.), no rewriting is performed - the query is simply wrapped in a list and returned.
2. **Regular Queries**: For DML statements (SELECT, INSERT, UPDATE, DELETE), the function calls  to apply the full rewrite system.

The function includes extensive debugging support with optional checks for:
- Parse tree copying verification (COPY_PARSE_PLAN_TREES)
- Serialization/deserialization testing (WRITE_READ_PARSE_PLAN_TREES)
- Debug output for both original and rewritten parse trees

Performance statistics can be collected when  is enabled.

## Parameters / Member Variables
- : Query structure from parse analysis that needs to be rewritten

## Dependencies
- Functions called/Symbols referenced:
  - [QueryRewrite](../Q/QueryRewrite.md)
  - elog_node_display
  - ResetUsage
  - ShowUsage
  - copyObject
  - [equal](../e/equal.md)
  - [nodeToStringWithLocations](../n/nodeToStringWithLocations.md)
  - [stringToNodeWithLocations](../s/stringToNodeWithLocations.md)
  - list_make1
- Called from (representative examples):
  - [pg_analyze_and_rewrite_fixedparams](pg_analyze_and_rewrite_fixedparams.md)
  - [pg_analyze_and_rewrite_varparams](pg_analyze_and_rewrite_varparams.md)
  - [pg_analyze_and_rewrite_withcb](pg_analyze_and_rewrite_withcb.md)
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [inline_set_returning_function](../i/inline_set_returning_function.md)

## Notes and Other Information
- The function assumes the input query comes directly from the parser (no AcquireRewriteLocks() is performed)
- Includes comprehensive debugging infrastructure for development and testing
- Critical component in PostgreSQL's multi-phase query processing architecture
- Located in src/backend/tcop/postgres.c:808-889
- The rewrite system is essential for handling PostgreSQL's advanced features like views and rules
- Performance can be monitored via the log_parser_stats configuration parameter
- Debug output can be controlled via Debug_print_parse and Debug_print_rewritten parameters