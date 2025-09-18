# pg_analyze_and_rewrite_withcb

## Location
src/backend/tcop/postgres.c: 768 - 807

## Overview
Performs parse analysis and query rewriting with a custom parser callback hook, providing a flexible interface for external parameter resolution and other parser customizations.

## Definition


## Detailed Description
This function is the core entry point for SQL query processing that transforms a raw parse tree into a list of rewritten query trees. It performs two main phases:

1. **Parse Analysis**: Converts the raw parse tree into a Query structure using , which allows for custom parameter resolution through the provided parser setup hook.

2. **Query Rewriting**: Applies PostgreSQL's query rewriter to handle views, rules, and other transformations via .

The function supports performance tracking through  and includes DTrace/SystemTap tracing points for query rewrite operations. This variant differs from  by accepting a parser callback instead of a fixed parameter list, providing greater flexibility for advanced use cases.

## Parameters / Member Variables
- : Raw parse tree structure containing the parsed SQL statement
- : Original SQL query string for logging and error reporting
- : Parser setup hook function for custom parameter resolution
- : Argument to pass to the parser setup hook
- : Query environment context for CTE handling and other environmental factors

## Dependencies
- Functions called/Symbols referenced:
  - [parse_analyze_withcb](parse_analyze_withcb.md)
  - [pg_rewrite_query](pg_rewrite_query.md)
  - ResetUsage
  - ShowUsage
  - TRACE_POSTGRESQL_QUERY_REWRITE_START
  - TRACE_POSTGRESQL_QUERY_REWRITE_DONE
- Called from (representative examples):
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [_SPI_prepare_plan](../S/_SPI_prepare_plan.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [inline_set_returning_function](../i/inline_set_returning_function.md)
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md)

## Notes and Other Information
- This function is critical in the PostgreSQL query processing pipeline, serving as a bridge between raw parsing and query planning
- The parser callback mechanism enables advanced features like prepared statements with dynamic parameter types
- Performance statistics collection can be enabled via the  configuration parameter
- The function is located in src/backend/tcop/postgres.c:768-807
- DTrace/SystemTap integration provides runtime query rewrite tracing capabilities