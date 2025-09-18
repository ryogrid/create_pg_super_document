# addHLParsedLex

## Location
src/backend/tsearch/ts_parse.c: 499 - 539

## Overview
A static function that processes parsed lexemes and normalized forms, adding them to the HeadlineParsedText structure while managing position tracking and memory cleanup.

## Definition
```c
static void addHLParsedLex(HeadlineParsedText *prs, TSQuery query, ParsedLex *lexs, TSLexeme *norms)
```

## Detailed Description
The `addHLParsedLex` function is a key component in the headline generation pipeline that processes both raw parsed lexemes and their normalized forms. It iterates through the linked list of ParsedLex entries, adding appropriate words to the headline structure using `hladdword`. For each entry, it also processes the corresponding normalized lexemes, calling `hlfinditem` to establish associations with query terms. The function carefully manages position tracking for text search vectors and handles memory cleanup for both lexeme lists and normalized forms.

## Parameters / Member Variables
- `prs`: Pointer to HeadlineParsedText structure where processed words are stored
- `query`: TSQuery structure containing search terms to match against
- `lexs`: Linked list of ParsedLex structures containing raw lexeme data
- `norms`: Array of TSLexeme structures containing normalized lexeme forms

## Dependencies
- Functions called/Symbols referenced:
  - hladdword (for adding words to the headline structure)
  - hlfinditem (for associating normalized lexemes with query items)
  - pfree (for memory cleanup)
  - strlen (for getting lexeme length)
- Data structures used:
  - HeadlineParsedText
  - TSQuery
  - ParsedLex
  - TSLexeme
- Constants used:
  - TSL_ADDPOS (flag indicating position should be incremented)
- Called from (representative examples):
  - hlparsetext

## Notes and Other Information
- This is a static function, accessible only within ts_parse.c
- The function handles two distinct phases: processing ParsedLex entries and processing normalized forms
- Position tracking is carefully managed using the TSL_ADDPOS flag to maintain accurate text vector positions
- Memory management is handled systematically - the function frees ParsedLex nodes as it processes them
- After processing, it cleans up the entire norms array and individual lexeme strings
- The function serves as a bridge between the parsing phase and the headline generation phase
- It ensures that both raw and normalized forms are properly integrated into the final headline structure
- The separation of lexs and norms processing allows for different handling of original tokens vs. normalized search terms