# _complete_from_query

## Location
src/bin/psql/tab-complete.c: 5250 - 5596

## Overview
Core function that performs tab completion for PostgreSQL commands by executing database queries and processing results to generate completion candidates.

## Definition
```c
static char *_complete_from_query(const char *simple_query,
                                 const SchemaQuery *schema_query,
                                 const char *const *keywords,
                                 bool verbatim,
                                 const char *text, int state)
```

## Detailed Description
This is the workhorse function of psql's tab completion system. It accepts two types of queries: simple queries with LIKE patterns and complex schema queries for database objects. The function handles the complete lifecycle of tab completion including parsing user input, constructing appropriate SQL queries, executing them against the PostgreSQL server, and formatting results for readline. It supports both qualified (schema.object) and unqualified object names, handles proper SQL identifier quoting, implements system catalog filtering (suppressing pg_* objects unless explicitly requested), and provides keyword completion alongside query results.

Key features include:
- Parses partially-typed identifiers into schema and object components
- Constructs dynamic SQL queries with proper escaping and LIKE patterns
- Supports reference objects through completion_ref_object/completion_ref_schema
- Handles both verbatim and identifier-aware completion modes
- Implements intelligent quoting based on user input patterns
- Provides fallback to keyword completion when query results are exhausted

## Parameters / Member Variables
- `simple_query`: A sprintf-style format string for simple completion queries with %s placeholders
- `schema_query`: A SchemaQuery structure defining complex schema-aware completion behavior
- `keywords`: Null-terminated array of literal keywords to include in completion
- `verbatim`: If true, return matches as-is; if false, parse and quote SQL identifiers appropriately
- `text`: The partial text being completed (from readline)
- `state`: Completion state (0 for first call, non-zero for subsequent calls)

## Dependencies
- Functions called/Symbols referenced:
  - [SchemaQuery](../S/SchemaQuery.md) (struct type)
  - [PQExpBufferData](../P/PQExpBufferData.md) (struct type)
  - [parse_identifier](../p/parse_identifier.md)
  - [make_like_pattern](../m/make_like_pattern.md)
  - [escape_string](../e/escape_string.md)
  - initPQExpBuffer
  - [exec_query](../e/exec_query.md)
  - termPQExpBuffer
  - PGRES_TUPLES_OK
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQnfields](../P/PQnfields.md)
  - [identifier_needs_quotes](../i/identifier_needs_quotes.md)
  - [requote_identifier](../r/requote_identifier.md)
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [pg_strdup_keyword_case](../p/pg_strdup_keyword_case.md)
- Called from (representative examples):
  - [complete_from_query](complete_from_query.md)
  - [complete_from_versioned_query](complete_from_versioned_query.md)
  - [complete_from_schema_query](complete_from_schema_query.md)
  - [complete_from_versioned_schema_query](complete_from_versioned_schema_query.md)

## Notes and Other Information
- Uses static variables to maintain state across multiple readline calls for the same completion
- Implements sophisticated logic for system catalog suppression (hides pg_* objects unless input starts with 'pg_')
- Supports up to three %s substitutions in simple queries: object pattern, reference object, reference schema
- Limits result sets using completion_max_records to prevent overwhelming the user
- Implements a hack to prevent readline from adding spaces after bare schema names
- Handles memory management carefully to prevent leaks across completion sessions
- The function is static and quite large (340+ lines), indicating its central importance to the completion system