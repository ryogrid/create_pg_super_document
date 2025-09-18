# prs_process_call

## Location
src/backend/tsearch/wparser.c: 216 - 241

## Overview
A static helper function that processes individual token results during text search parsing operations, converting parsed tokens into tuple format for return to the caller.

## Definition
```c
static Datum prs_process_call(FuncCallContext *funcctx)
```

## Detailed Description
This function serves as the core iteration handler for PostgreSQL's text search parser functions. It operates within a function call context to process parsed tokens one by one, converting each token into a properly formatted tuple containing the token type and lexeme. The function maintains state through a PrsStorage structure and advances through the token list sequentially until all tokens are processed.

The function builds tuples with two string values: the numeric token type identifier and the actual lexeme text. Each processed token's memory is properly freed after tuple creation to prevent memory leaks.

## Parameters / Member Variables
- `funcctx`: Function call context containing the parsing state and metadata for tuple construction

## Dependencies
- Functions called/Symbols referenced:
  - FuncCallContext (PostgreSQL function context structure)
  - PrsStorage (token storage structure)
  - BuildTupleFromCStrings (tuple construction utility)
  - HeapTupleGetDatum (tuple to Datum conversion)
  - sprintf (standard C string formatting)
  - pfree (PostgreSQL memory deallocation)

- Called from (representative examples):
  - ts_parse_byid (at src/backend/tsearch/wparser.c:258)
  - ts_parse_byname (at src/backend/tsearch/wparser.c:282)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the wparser.c module
- Returns (Datum) 0 when all tokens have been processed, signaling end of results
- Responsible for proper memory management by freeing lexeme strings after tuple creation
- Works in conjunction with prs_setup_firstcall for complete parser function implementation
- Token types are converted to string format using sprintf for tuple compatibility