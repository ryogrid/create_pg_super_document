# ts_parse_byname

## Location
src/backend/tsearch/wparser.c: 264 - 287

## Overview
A PostgreSQL function that parses text using a specified text search parser identified by its name, returning tokens as a set of rows with token type and lexeme.

## Definition
```c
Datum ts_parse_byname(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a set-returning function (SRF) that parses input text using a text search parser identified by its name rather than OID. It follows the standard PostgreSQL SRF pattern with first-call initialization and per-call processing.

On the first call, the function converts the parser name to a qualified name list, resolves it to an OID using get_ts_parser_oid, then initializes the parsing context using prs_setup_firstcall. On subsequent calls, it uses prs_process_call to return individual tokens until all tokens are exhausted.

This function is essentially identical to ts_parse_byid except that it accepts a parser name instead of a direct OID, providing a more user-friendly interface for text parsing operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `PG_GETARG_TEXT_PP(0)`: The name of the text search parser to use
  - `PG_GETARG_TEXT_PP(1)`: The input text to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - FuncCallContext (PostgreSQL SRF context structure)
  - SRF_IS_FIRSTCALL (macro to check if this is the first call)
  - SRF_FIRSTCALL_INIT (macro to initialize first call context)
  - get_ts_parser_oid (resolves parser name to OID)
  - textToQualifiedNameList (converts text to qualified name list)
  - prs_setup_firstcall (initializes parser and tokenizes text)
  - SRF_PERCALL_SETUP (macro to set up per-call context)
  - prs_process_call (processes individual tokens)
  - SRF_RETURN_NEXT (macro to return next result)
  - SRF_RETURN_DONE (macro to signal completion)

- Called from (representative examples):
  - This is a top-level PostgreSQL function, typically called from SQL queries

## Notes and Other Information
- This function is part of PostgreSQL's text search functionality
- Uses the standard SRF (Set Returning Function) pattern for returning multiple rows
- More user-friendly than ts_parse_byid as it accepts parser names instead of OIDs
- The parser name is resolved to an OID during the first call using PostgreSQL's catalog system
- Returns tuples with two columns: token type (integer) and lexeme (text)
- Companion function to ts_parse_byid which takes a parser OID instead of name
- The parser name can be schema-qualified (e.g., 'pg_catalog.default')