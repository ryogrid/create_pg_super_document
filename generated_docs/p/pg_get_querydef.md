# pg_get_querydef

## Location
src/backend/utils/adt/ruleutils.c: 1568 - 1587

## Overview
Public entry point function that decompiles a PostgreSQL query parse tree into a readable SQL statement string with configurable formatting options.

## Definition
```c
char *pg_get_querydef(Query *query, bool pretty)
```

## Detailed Description
This function serves as the main public interface for converting internal PostgreSQL Query structures back into human-readable SQL statements. It acts as a wrapper around the core get_query_def function, handling the setup of formatting parameters and output buffer management.

The function takes a query parse tree (which represents the internal parsed and analyzed form of an SQL statement) and reconstructs the equivalent SQL text. This is particularly useful for debugging, logging, displaying query plans, and implementing features like view definitions or stored procedure bodies.

The function initializes a StringInfo buffer, sets up pretty-printing flags based on the input parameter, and delegates the actual decompilation work to get_query_def with standard default parameters for most use cases.

## Parameters / Member Variables
- `query`: Pointer to a Query structure containing the parsed and analyzed SQL statement to be decompiled
- `pretty`: Boolean flag indicating whether to format the output with pretty formatting (affects indentation, line breaks, and spacing)

## Dependencies
- Functions called/Symbols referenced:
  - GET_PRETTY_FLAGS (macro for converting boolean pretty flag to internal formatting flags)
  - initStringInfo (initializes string buffer for output)
  - [get_query_def](../g/get_query_def.md) (core function that performs the actual query decompilation)
  - WRAP_COLUMN_DEFAULT (constant defining default column wrapping behavior)
- Called from (representative examples):
  - Referenced by RULE_INDEXDEF_KEYS_ONLY constant (in src/include/utils/ruleutils.h)

## Notes and Other Information
- This is the primary public interface for query decompilation in PostgreSQL's rule utilities system
- Returns a palloc'd C string that must be freed by the caller when no longer needed
- The function uses standard defaults for most decompilation parameters: no namespace list, no variable table, top-level context, and default column wrapping
- Part of PostgreSQL's broader rule utilities framework that handles decompilation of various database objects
- The pretty formatting option affects readability but not semantic content of the generated SQL
- Commonly used by PostgreSQL internals for generating human-readable representations of stored queries, views, and rules