# hlparsetext

## Location
src/backend/tsearch/ts_parse.c: 540 - 606

## Overview
A core function that parses input text using PostgreSQL's text search framework and populates a HeadlineParsedText structure with processed lexemes and their associations with query terms.

## Definition
```c
void hlparsetext(Oid cfgId, HeadlineParsedText *prs, TSQuery query, char *buf, int buflen)
```

## Detailed Description
The `hlparsetext` function serves as the main entry point for parsing text during headline generation in PostgreSQL's full-text search system. It initializes and utilizes the text search configuration's parser to tokenize input text, then processes each token through the lexicalization pipeline. The function handles parser setup, token extraction, length validation, lexeme normalization, and integration with the headline structure. It manages the complete parsing workflow from raw text to structured lexeme data ready for headline generation.

## Parameters / Member Variables
- `cfgId`: Object identifier of the text search configuration to use for parsing
- `prs`: Pointer to HeadlineParsedText structure to populate with parsed results
- `query`: TSQuery structure containing search terms for matching
- `buf`: Character buffer containing the input text to parse
- `buflen`: Length of the input text buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - lookup_ts_config_cache (retrieve cached configuration)
  - lookup_ts_parser_cache (retrieve cached parser)
  - FunctionCall2/FunctionCall3/FunctionCall1 (PostgreSQL function calling interface)
  - DatumGetInt32/DatumGetPointer/PointerGetDatum/Int32GetDatum (datum conversion)
  - LexizeInit (initialize lexicalization)
  - LexizeAddLemm (add lemma to lexicalization data)
  - LexizeExec (execute lexicalization)
  - addHLParsedLex (add processed lexemes to headline structure)
  - ereport (error reporting)
- Data structures used:
  - HeadlineParsedText
  - TSQuery
  - LexizeData
  - TSLexeme
  - ParsedLex
  - TSConfigCacheEntry
  - TSParserCacheEntry
- Constants used:
  - MAXSTRLEN (maximum lexeme length)
  - IGNORE_LONGLEXEME (compilation flag for handling long lexemes)
- Called from (representative examples):
  - ts_headline_byid_opt
  - headline_json_value

## Notes and Other Information
- This is a public function (not static), accessible from other translation units
- The function implements a two-level parsing loop: outer loop for tokens, inner loop for normalized forms
- Length validation prevents indexing of excessively long words (>MAXSTRLEN characters)
- Error handling behavior for long words depends on IGNORE_LONGLEXEME compilation flag
- The function maintains proper resource management by calling parser start/end functions
- Position tracking (vectorpos) is maintained throughout the parsing process
- Each successful lexicalization increments the vector position counter
- The function handles cases where lexicalization produces no normalized forms (NULL norms)
- Parser data is managed through PostgreSQL's function call interface for extensibility
- Memory management is handled by the called functions (LexizeExec, addHLParsedLex)
- This function is fundamental to PostgreSQL's ts_headline functionality