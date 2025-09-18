# init_tsvector_parser

## Location
src/backend/utils/adt/tsvector_parser.c: 57 - 80

## Overview
Initializes a parser state object for parsing tsvector or tsquery input strings with configurable flags and error handling context.

## Definition


## Detailed Description
This function creates and initializes a TSVectorParseState structure that maintains the parsing state for tsvector and tsquery operations. It allocates memory for the parser state and sets up initial values based on the provided flags. The parser supports different parsing modes including operator-as-delimiter mode, tsquery mode, and web search mode. The function also configures error handling through the provided error context.

## Parameters / Member Variables
- : The input string to be parsed
- : A bitmask of parsing flags from ts_utils.h (P_TSV_OPR_IS_DELIM, P_TSV_IS_TSQUERY, P_TSV_IS_WEB)
- : Error context node for soft error handling, can be NULL for hard errors

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - pg_database_encoding_max_length
  - TSVectorParseState
  - TSVectorParseStateData
  - P_TSV_OPR_IS_DELIM
  - P_TSV_IS_TSQUERY  
  - P_TSV_IS_WEB
- Called from (representative examples):
  - tsvectorin (src/backend/utils/adt/tsvector.c:202)
  - parse_tsquery (src/backend/utils/adt/tsquery.c:859)

## Notes and Other Information
The parser allocates an initial word buffer of 32 characters that can be expanded during parsing. The encoding maximum length is cached for efficient character processing. The parser state must be properly cleaned up using close_tsvector_parser() after use to avoid memory leaks.