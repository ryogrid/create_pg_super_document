# close_tsvector_parser

## Location
src/backend/utils/adt/tsvector_parser.c: 90 - 96

## Overview
Shuts down a tsvector parser by freeing all allocated memory associated with the parser state.

## Definition


## Detailed Description
This function performs cleanup operations for a TSVectorParseState object by deallocating all memory that was allocated during parser initialization. It specifically frees the word buffer and the parser state structure itself. This function must be called after parsing is complete to prevent memory leaks.

## Parameters / Member Variables
- : The TSVectorParseState object to clean up and deallocate

## Dependencies
- Functions called/Symbols referenced:
  - pfree
  - TSVectorParseState
- Called from (representative examples):
  - tsvectorin (src/backend/utils/adt/tsvector.c:260)
  - parse_tsquery (src/backend/utils/adt/tsquery.c:870)

## Notes and Other Information
This function should always be called in a try-catch or PG_ENSURE_ERROR_CLEANUP block to guarantee cleanup even when errors occur during parsing. The parser state becomes invalid after calling this function and should not be accessed again.