# ts_token_type_byid

## Location
src/backend/tsearch/wparser.c: 106 - 124

## Overview
A PostgreSQL SQL-callable function that returns token type information for a specified text search parser as a set of rows.

## Definition


## Detailed Description
This function provides a SQL interface for retrieving token type information from PostgreSQL text search parsers. It implements the Set Returning Function (SRF) protocol to return multiple rows of token type data, where each row contains the token ID, alias, and description. The function takes a parser OID as input and uses the parser's lextype method to obtain the complete list of supported token types.

The function follows PostgreSQL's standard SRF pattern:
1. On the first call, it initializes the function context and retrieves token type data
2. On subsequent calls, it returns the next token type in the sequence
3. When all token types have been returned, it signals completion

This function is typically called from SQL queries to inspect the token types supported by a particular text search parser, which is useful for understanding how the parser categorizes different types of text elements.

## Parameters / Member Variables
- Function receives parser OID through  - the OID of the text search parser to query

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - [tt_setup_firstcall](tt_setup_firstcall.md)
  - SRF_PERCALL_SETUP
  - [tt_process_call](tt_process_call.md)
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
- Called from (representative examples):
  - Direct SQL calls from user queries
  - System catalog queries

## Notes and Other Information
- This is a public function accessible from SQL as part of PostgreSQL's text search functionality
- Uses PostgreSQL's Set Returning Function macros for proper multi-call protocol implementation
- The function name follows PostgreSQL's naming convention for text search functions (ts_*)
- Returns a composite type with columns for token ID, alias, and description
- Requires a valid text search parser OID as input parameter
- Part of PostgreSQL's full-text search infrastructure for parser introspection