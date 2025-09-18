# LexizeExec

## Location
[src/backend/tsearch/ts_parse.c:173-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L173-L354)

## Overview
Executes lexical analysis on parsed tokens using configured dictionaries, handling both single-word and multi-word dictionary processing modes.

## Definition
```c
static TSLexeme *LexizeExec(LexizeData *ld, ParsedLex **correspondLexem)
```

## Detailed Description
LexizeExec is the core function for lexical analysis in PostgreSQL's text search system. It processes ParsedLex tokens through a configuration of dictionaries, operating in two distinct modes:

**Normal Mode (curDictId == InvalidOid):**
- Processes tokens from the towork queue sequentially
- For each token type, tries all configured dictionaries in order
- If a dictionary sets getnext=true, switches to multi-word mode
- Returns the first successful lexeme result

**Multi-word Mode (curDictId is valid):**
- A specific dictionary is requesting additional words to form compound lexemes
- Continues processing subsequent tokens with the same dictionary
- Validates that the dictionary can handle each token type
- Returns to normal mode when the dictionary finishes or fails

The function manages state through the LexizeData structure and can recursively call itself when switching between modes. It handles filtering lexemes (TSL_FILTER), temporary results, and proper cleanup of processed tokens.

## Parameters / Member Variables
- `ld`: Pointer to LexizeData structure containing parsing state, configuration, and work queues
- `correspondLexem`: Double pointer to receive the list of processed lexemes that generated the result

## Dependencies
- Functions called/Symbols referenced:
  - RemoveHead (removes processed tokens from work queue)
  - [lookup_ts_dictionary_cache](../l/lookup_ts_dictionary_cache.md) (retrieves dictionary cache entries)
  - FunctionCall4 (calls dictionary lexize functions)
  - [setNewTmpRes](../s/setNewTmpRes.md) (stores temporary results during multi-word processing)
  - [moveToWaste](../m/moveToWaste.md) (moves processed tokens to waste list)
  - [setCorrLex](../s/setCorrLex.md) (manages corresponding lexeme list)
  - [DatumGetObjectId](../D/DatumGetObjectId.md) (extracts ObjectId from Datum)
- Called from (representative examples):
  - [parsetext](../p/parsetext.md) (main text parsing function at line 402)
  - [hlparsetext](../h/hlparsetext.md) (highlighting text parsing at line 590)
  - Self-recursive calls (lines 226, 288, 341)

## Notes and Other Information
- This is a static function, only accessible within the ts_parse.c compilation unit
- The function implements a finite state machine for dictionary processing
- Recursive calls handle mode transitions and state resets
- Supports filtering lexemes where dictionaries can transform input before further processing
- Multi-word processing allows dictionaries to build compound terms from multiple tokens
- Proper memory management ensures temporary results are cleaned up
- The function can handle dictionaries that don't recognize certain lexeme types by skipping them
- State management is crucial for maintaining consistency across recursive calls