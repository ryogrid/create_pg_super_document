# raw_parser

## Location
src/backend/parser/parser.c: 42 - 110

## Overview
The primary entry point for PostgreSQL's SQL parser that performs lexical and grammatical analysis on query strings, returning a list of raw (unanalyzed) parse trees.

## Definition


## Detailed Description
The raw_parser function serves as the main interface to PostgreSQL's parsing subsystem. It takes a SQL query string and performs both lexical scanning (tokenization) and grammatical parsing to produce an Abstract Syntax Tree (AST). The function supports different parsing modes to handle various contexts like type names, PL/pgSQL expressions, and assignments.

The parsing process involves:
1. Initializing the flex scanner with the input string
2. Setting up mode-specific lookahead tokens for different parsing contexts
3. Initializing the bison parser
4. Executing the parse operation
5. Cleaning up scanner resources
6. Returning the resulting parse tree list

## Parameters / Member Variables
- : The input SQL query string to be parsed
- : The parsing mode (RawParseMode enum) that determines the parsing context and behavior

## Dependencies
- Functions called/Symbols referenced:
  - scanner_init
  - parser_init
  - base_yyparse
  - scanner_finish
  - RawParseMode (enum)
  - core_yyscan_t (type)
  - base_yy_extra_type (type)
- Called from (representative examples):
  - ATPostAlterTypeParse
  - _SPI_prepare_plan
  - _SPI_prepare_oneshot_plan
  - typeStringToTypeName
  - pg_parse_query

## Notes and Other Information
- Returns NIL on parsing errors
- Supports multiple parsing modes including default SQL, type names, and PL/pgSQL expressions
- The function handles memory management by cleaning up scanner resources regardless of parse success or failure
- Mode-specific lookahead tokens are used to provide context to the parser for specialized parsing scenarios