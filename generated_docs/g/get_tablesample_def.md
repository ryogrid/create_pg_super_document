# get_tablesample_def

## Location
[src/backend/utils/adt/ruleutils.c:12487-12530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12487-L12530)

## Overview
Generates SQL text for a TABLESAMPLE clause, including the sampling method, arguments, and optional REPEATABLE specification.

## Definition
```c
static void get_tablesample_def(TableSampleClause *tablesample, deparse_context *context)
```

## Detailed Description
This function reconstructs the SQL syntax for table sampling clauses, which allow users to retrieve a random sample of rows from a table. The function handles the complete TABLESAMPLE syntax including:

1. **Method name**: Resolves and properly qualifies the table sampling method function name
2. **Arguments**: Processes and formats the sampling method arguments (e.g., percentage, number of rows)
3. **REPEATABLE clause**: Optionally includes the REPEATABLE specification for deterministic sampling

The function generates output in the format:
`TABLESAMPLE method_name(arg1, arg2) REPEATABLE (seed_value)`

Key features:
- Properly qualifies the sampling handler function name based on the current search path
- Handles variable numbers of arguments to the sampling method
- Includes REPEATABLE clause only when specified
- Uses get_rule_expr to format complex argument expressions

## Parameters / Member Variables
- `tablesample`: TableSampleClause structure containing sampling method, arguments, and repeatability information
- `context`: Deparse context containing output buffer and namespace information

## Dependencies
- Functions called/Symbols referenced:
  - [generate_function_name](generate_function_name.md)
  - [get_rule_expr](get_rule_expr.md)
  - [appendStringInfo](../a/appendStringInfo.md), appendStringInfoString, appendStringInfoChar
- Called from (representative examples):
  - [get_from_clause_item](get_from_clause_item.md) (for relations with TABLESAMPLE clauses)

## Notes and Other Information
- Part of PostgreSQL's statistical sampling functionality introduced in version 9.5+
- Handles both built-in sampling methods (SYSTEM, BERNOULLI) and custom sampling methods
- The function uses INTERNALOID as the argument type for function name resolution
- Essential for reconstructing queries with table sampling for query plan display and rule definitions
- REPEATABLE clause ensures deterministic sampling results when the same seed is used
- Must appear after table aliases in FROM clause syntax according to SQL standard