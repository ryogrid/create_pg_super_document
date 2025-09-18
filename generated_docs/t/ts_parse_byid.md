# ts_parse_byid

## Location
src/backend/tsearch/wparser.c: 242 - 263

## Overview
A PostgreSQL function that parses text using a specified text search parser by its OID, returning tokens as a set of rows with token type and lexeme.

## Definition
```c
Datum ts_parse_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a set-returning function (SRF) that parses input text using a text search parser identified by its Object Identifier (OID). It follows the standard PostgreSQL SRF pattern with first-call initialization and per-call processing.

On the first call, the function initializes the parsing context using prs_setup_firstcall, which sets up the parser and tokenizes the input text. On subsequent calls, it uses prs_process_call to return individual tokens until all tokens are exhausted.

The function expects two arguments: a parser OID and the text to be parsed. It returns a set of rows where each row contains the token type (as an integer) and the corresponding lexeme (as text).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `PG_GETARG_OID(0)`: The OID of the text search parser to use
  - `PG_GETARG_TEXT_PP(1)`: The input text to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - FuncCallContext (PostgreSQL SRF context structure)
  - SRF_IS_FIRSTCALL (macro to check if this is the first call)
  - SRF_FIRSTCALL_INIT (macro to initialize first call context)
  - prs_setup_firstcall (initializes parser and tokenizes text)
  - SRF_PERCALL_SETUP (macro to set up per-call context)
  - prs_process_call (processes individual tokens)
  - SRF_RETURN_NEXT (macro to return next result)
  - SRF_RETURN_DONE (macro to signal completion)
  - PG_FREE_IF_COPY (memory management for varlena types)

- Called from (representative examples):
  - This is a top-level PostgreSQL function, typically called from SQL queries

## Notes and Other Information
- This function is part of PostgreSQL's text search functionality
- Uses the standard SRF (Set Returning Function) pattern for returning multiple rows
- Proper memory management with PG_FREE_IF_COPY for the input text parameter
- The parser OID must correspond to a valid text search parser in the system catalog
- Returns tuples with two columns: token type (integer) and lexeme (text)
- Companion function to ts_parse_byname which takes a parser name instead of OID