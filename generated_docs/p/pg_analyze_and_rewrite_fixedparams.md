# pg_analyze_and_rewrite_fixedparams

## Location
[src/backend/tcop/postgres.c:675-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L675-L713)

## Overview
Performs parse analysis and rule rewriting on a raw parse tree with fixed parameter types, transforming it into a list of executable Query nodes.

## Definition
```c
List *pg_analyze_and_rewrite_fixedparams(RawStmt *parsetree,
                                        const char *query_string,
                                        const Oid *paramTypes,
                                        int numParams,
                                        QueryEnvironment *queryEnv)
```

## Detailed Description
pg_analyze_and_rewrite_fixedparams is a critical function in PostgreSQL's query processing pipeline that bridges the gap between raw parsing and query execution. It takes a raw parse tree (output from the grammar parser) and transforms it through two essential stages: parse analysis and rule rewriting. The function is specifically designed for scenarios where parameter types are known in advance (fixed parameters), such as in prepared statements or stored procedures.

The function operates in two distinct phases: first, it performs semantic analysis via parse_analyze_fixedparams(), which validates the query structure, resolves table and column references, performs type checking, and transforms the raw parse tree into a semantically valid Query node. Second, it applies PostgreSQL's rule system through pg_rewrite_query(), which can expand a single query into multiple queries based on defined rules (such as views, RLS policies, or user-defined rules).

Performance statistics can be logged if log_parser_stats is enabled, and the function supports query environment contexts for advanced query processing scenarios.

## Parameters / Member Variables
- `parsetree`: Raw parse tree (RawStmt) from the grammar parser to be analyzed and rewritten
- `query_string`: Original SQL query string for error reporting and logging purposes
- `paramTypes`: Array of parameter type OIDs for resolving  parameter references
- `numParams`: Number of parameters in the paramTypes array
- `queryEnv`: Query environment providing context for query processing (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [parse_analyze_fixedparams](parse_analyze_fixedparams.md)
  - [pg_rewrite_query](pg_rewrite_query.md)
  - ResetUsage
  - ShowUsage
  - RawStmt (type)
  - QueryEnvironment (type)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [_SPI_prepare_plan](../S/_SPI_prepare_plan.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md)

## Notes and Other Information
- Returns a List of Query nodes since rewriting may expand one query into several
- Essential for prepared statements and parameterized queries where types are predetermined
- Must be separate from raw parsing due to database access requirements during analysis
- Cannot be executed in aborted transactions due to catalog access needs
- Part of PostgreSQL's sophisticated query transformation pipeline
- Supports performance monitoring and tracing for development and debugging
- Critical for maintaining query semantics and applying PostgreSQL's rule system correctly