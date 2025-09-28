# pg_analyze_and_rewrite_withcb

## Location
[src/backend/tcop/postgres.c:768-807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L768-L807)

## Overview
Performs parse analysis and query rewriting with a custom parser callback hook, providing a flexible interface for external parameter resolution and other parser customizations.

## Definition

```c
List *
pg_analyze_and_rewrite_withcb(RawStmt *parsetree,
							  const char *query_string,
							  ParserSetupHook parserSetup,
							  void *parserSetupArg,
							  QueryEnvironment *queryEnv)
```
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
  - [ResetUsage](../R/ResetUsage.md)
  - [ShowUsage](../S/ShowUsage.md)
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

## Simplified Source

```c
// Simplified version of pg_analyze_and_rewrite_withcb
List *pg_analyze_and_rewrite_withcb(RawStmt *parsetree,
                                   const char *query_string,
                                   ParserSetupHook parserSetup,
                                   void *parserSetupArg,
                                   QueryEnvironment *queryEnv) {
    Query *query;
    List *querytree_list;

    // Start tracing for query rewrite
    TRACE_POSTGRESQL_QUERY_REWRITE_START(query_string);

    // Phase 1: Parse analysis with custom callback
    if (log_parser_stats)
        ResetUsage();

    query = parse_analyze_withcb(parsetree, query_string, parserSetup,
                                parserSetupArg, queryEnv);

    if (log_parser_stats)
        ShowUsage("PARSE ANALYSIS STATISTICS");

    // Phase 2: Query rewriting
    querytree_list = pg_rewrite_query(query);

    // End tracing
    TRACE_POSTGRESQL_QUERY_REWRITE_DONE(query_string);

    return querytree_list;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Focused on the two main phases: parse analysis and rewriting
- Maintained performance tracking and tracing capabilities
- Streamlined the function flow while preserving essential functionality