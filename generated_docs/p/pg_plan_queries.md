# pg_plan_queries

## Location
src/backend/tcop/postgres.c: 976 - 1016

## Overview
Generates execution plans for a list of already-rewritten queries, handling both regular optimizable statements and utility commands to produce a list of PlannedStmt nodes.

## Definition


## Detailed Description
This function serves as a batch planning interface that processes multiple queries at once, which is common when dealing with complex SQL statements that may be rewritten into multiple query trees. The function handles two distinct types of queries:

1. **Utility Commands (CMD_UTILITY)**: For DDL statements and other utility commands, the function creates a simple wrapper PlannedStmt node without invoking the planner. It preserves important metadata from the original query including:
   - Command type and tag setting capability
   - Utility statement reference
   - Statement location and length information
   - Query identifier for tracking

2. **Regular Queries**: For DML statements (SELECT, INSERT, UPDATE, DELETE), the function delegates to  to perform full cost-based optimization planning.

The function iterates through each query in the input list, determines the appropriate planning strategy, and builds a corresponding list of PlannedStmt nodes that can be executed by PostgreSQL's executor.

## Parameters / Member Variables
- : List of rewritten Query structures ready for planning
- : Original SQL query string for logging and error reporting
- : Cursor-specific options affecting plan generation
- : Parameter values for prepared statement planning

## Dependencies
- Functions called/Symbols referenced:
  - pg_plan_query
  - makeNode
  - lfirst_node
  - lappend
- Called from (representative examples):
  - exec_simple_query
  - execute_sql_string
  - BuildCachedPlan

## Notes and Other Information
- Essential for handling multi-statement queries and complex rewrite scenarios
- Efficiently batches planning operations while maintaining individual query context
- Preserves all necessary metadata for utility commands without unnecessary planning overhead
- Critical component in PostgreSQL's query processing pipeline, bridging rewriting and execution phases
- Located in src/backend/tcop/postgres.c:976-1016
- The function maintains the order of queries in the input list in the output planned statement list
- Utility commands bypass the planner entirely, improving performance for DDL operations
- Used extensively in prepared statement caching and extension SQL execution contexts