# ts_token_type_byname

## Location
src/backend/tsearch/wparser.c: 125 - 150

## Overview
A PostgreSQL SQL-callable function that returns token type information for a text search parser specified by name as a set of rows.

## Definition


## Detailed Description
This function provides a SQL interface for retrieving token type information from PostgreSQL text search parsers using the parser's name instead of its OID. Like , it implements the Set Returning Function (SRF) protocol to return multiple rows of token type data, where each row contains the token ID, alias, and description. The key difference is that this function accepts a text parser name as input and resolves it to the corresponding parser OID before proceeding.

The function workflow:
1. On the first call, it converts the parser name to a qualified name list
2. Resolves the parser name to its OID using the system catalog
3. Initializes the function context and retrieves token type data using the resolved OID
4. On subsequent calls, it returns the next token type in the sequence
5. When all token types have been returned, it signals completion

This function provides a more user-friendly interface compared to  since users typically know parser names rather than internal OIDs.

## Parameters / Member Variables
- Function receives parser name through  - the name of the text search parser to query

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - [get_ts_parser_oid](../g/get_ts_parser_oid.md)
  - textToQualifiedNameList
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
- More user-friendly than  as it accepts parser names instead of OIDs
- Uses PostgreSQL's Set Returning Function macros for proper multi-call protocol implementation
- The parser name can be schema-qualified (e.g., 'pg_catalog.default') or simple (e.g., 'default')
- Throws an error if the specified parser name cannot be found in the system catalog
- Returns the same composite type structure as  with columns for token ID, alias, and description
- Part of PostgreSQL's full-text search infrastructure for parser introspection