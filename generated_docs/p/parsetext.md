# parsetext

## Location
[src/backend/tsearch/ts_parse.c:355-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L355-L439)

## Overview
Parses and lexically analyzes a text string using a specified text search configuration, producing normalized words with positional information.

## Definition
```c
void parsetext(Oid cfgId, ParsedText *prs, char *buf, int buflen)
```

## Detailed Description
The parsetext function is the main entry point for text processing in PostgreSQL's full-text search system. It orchestrates the complete pipeline from raw text to normalized, indexed words:

**Initialization Phase:**
- Looks up the text search configuration and associated parser
- Initializes the parser with the input buffer
- Sets up lexical analysis data structures

**Processing Loop:**
- Calls the parser to extract tokens from the input text
- For each token, validates length constraints (MAXSTRLEN)
- Adds tokens to the lexical analysis queue
- Processes tokens through configured dictionaries via LexizeExec
- Converts normalized lexemes to ParsedWord structures

**Output Management:**
- Dynamically expands the output array as needed
- Tracks positional information for each word
- Handles lexeme variants and flags (TSL_ADDPOS, TSL_PREFIX)
- Manages memory allocation and cleanup

The function stops processing when the parser returns type <= 0, indicating end of input, and properly cleans up parser resources.

## Parameters / Member Variables
- `cfgId`: Object ID of the text search configuration to use for parsing
- `prs`: Pointer to ParsedText structure that will be filled with results (words, positions, counts)
- `buf`: Input text buffer to be parsed
- `buflen`: Length of the input buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md) (retrieves text search configuration)
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md) (retrieves parser configuration)
  - FunctionCall2, FunctionCall3, FunctionCall1 (calls parser functions)
  - LexizeInit (initializes lexical analysis)
  - LexizeAddLemm (adds tokens to lexical queue)
  - [LexizeExec](../L/LexizeExec.md) (processes tokens through dictionaries)
  - [repalloc](../r/repalloc.md) (reallocates memory for growing word array)
  - [DatumGetInt32](../D/DatumGetInt32.md), DatumGetPointer (type conversion functions)
- Called from (representative examples):
  - [to_tsvector_byid](../t/to_tsvector_byid.md) (main tsvector creation function)
  - [add_to_tsvector](../a/add_to_tsvector.md) (incremental tsvector building)
  - [pushval_morph](pushval_morph.md) (morphological processing)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md) (automatic tsvector updates)

## Notes and Other Information
- This is a public function used throughout the text search subsystem
- Handles extremely long words based on IGNORE_LONGLEXEME compilation flag
- Dynamically grows the output word array using a doubling strategy
- Maintains proper positional information for phrase searches
- Supports lexeme variants and special flags for advanced text search features
- Memory management follows PostgreSQL conventions with automatic cleanup
- The function is central to tsvector creation and text search indexing