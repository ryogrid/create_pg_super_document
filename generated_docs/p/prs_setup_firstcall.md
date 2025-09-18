# prs_setup_firstcall

## Location
src/backend/tsearch/wparser.c: 162 - 215

## Overview
Initializes function context and performs text parsing during the first call of PostgreSQL text search parser functions that tokenize input text.

## Definition


## Detailed Description
This function sets up the necessary data structures and performs the complete text parsing process for PostgreSQL text search parser functions during their first call. Unlike  which retrieves token type definitions, this function actually parses input text and stores all resulting lexemes (tokens) for subsequent retrieval. The function uses the parser's start, token, and end methods to process the entire input text and stores each lexeme with its type information.

The function performs these key operations:
1. Initializes a PrsStorage structure to hold parsing results
2. Calls the parser's start method to initialize parsing of the input text
3. Repeatedly calls the parser's token method to extract lexemes until parsing is complete
4. Dynamically resizes the lexeme storage array as needed
5. Calls the parser's end method to finalize parsing
6. Sets up tuple descriptor and attribute metadata for the return type

The parsing process extracts all tokens from the input text in a single pass during the first call, storing them in memory for efficient retrieval in subsequent function calls.

## Parameters / Member Variables
- : Function call context structure used for multi-call functions
- : Function call information containing metadata about the function call
- : OID of the text search parser to use for parsing
- : Input text to be parsed and tokenized

## Dependencies
- Functions called/Symbols referenced:
  - lookup_ts_parser_cache
  - FunctionCall2 (parser start method)
  - FunctionCall3 (parser token method)
  - FunctionCall1 (parser end method)
  - DatumGetInt32
  - repalloc
  - get_call_result_type
  - TupleDescGetAttInMetadata
  - MemoryContextSwitchTo
  - palloc
  - memcpy
- Called from (representative examples):
  - ts_parse_byid
  - ts_parse_byname

## Notes and Other Information
- This is a static function internal to the wparser.c module
- Performs complete text parsing during the first call, unlike token type functions that just retrieve metadata
- Dynamically resizes the lexeme storage array, starting with 16 entries and doubling as needed
- Uses PostgreSQL's multi-call function framework for returning sets of rows
- Memory allocation is done in the multi-call memory context to ensure persistence across calls
- Each lexeme is stored with its text content and token type for later retrieval
- The function properly manages parser lifecycle by calling start, token (repeatedly), and end methods
- Handles variable-length input text through VARDATA_ANY and VARSIZE_ANY_EXHDR macros