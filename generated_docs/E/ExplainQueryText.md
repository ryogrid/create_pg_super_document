# ExplainQueryText

## Location
[src/backend/commands/explain.c:1169-1183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1169-L1183)

## Overview
Adds the actual query text to EXPLAIN output as a "Query Text" property when the source text is available.

## Definition
```c
void ExplainQueryText(ExplainState *es, QueryDesc *queryDesc)
```

## Detailed Description
ExplainQueryText is a simple utility function that adds the original SQL query text to EXPLAIN output. It checks if the QueryDesc contains source text and, if present, adds it as a formatted property labeled "Query Text" to the explanation output.

This function provides a way to include the actual SQL statement being explained in the output, which can be useful for debugging, logging, or when the EXPLAIN output needs to be self-contained with the original query for reference.

The function assumes that the ExplainState has been properly initialized with output formatting options and that the output buffer (es->str) is ready for writing.

## Parameters / Member Variables
- `es`: ExplainState structure containing formatting configuration and output destination
- `queryDesc`: QueryDesc structure containing query execution state and the source text of the original SQL query

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainPropertyText](ExplainPropertyText.md) (formats and adds text property to output)
- Called from (representative examples):
  - Currently no direct callers found in the codebase (utility function for manual use)

## Notes and Other Information
- Public function (not static), available for use throughout the PostgreSQL backend
- Only adds output if queryDesc->sourceText is non-NULL
- Simple wrapper around ExplainPropertyText for consistent formatting
- Part of PostgreSQLs EXPLAIN infrastructure for displaying query information
- Useful for including the original query text in EXPLAIN output for reference
- Located in src/backend/commands/explain.c:1169-1183

## Simplified Source

```c
// Simplified version of ExplainQueryText
void ExplainQueryText(ExplainState *es, QueryDesc *queryDesc) {
    // Add query text to EXPLAIN output if available
    if (queryDesc->sourceText)
        ExplainPropertyText("Query Text", queryDesc->sourceText, es);
}
```

Key simplifications made:
- Simple function with minimal changes needed
- Added clear comment explaining the purpose
- Maintained the essential null-check and function call
- No complex logic to simplify - function is already very straightforward