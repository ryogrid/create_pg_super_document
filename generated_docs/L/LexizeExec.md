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
  - [RemoveHead](../R/RemoveHead.md) (removes processed tokens from work queue)
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

## Simplified Source

```c
static TSLexeme *LexizeExec(LexizeData *ld, ParsedLex **correspondLexem) {
    int i;
    ListDictionary *map;
    TSDictionaryCacheEntry *dict;
    TSLexeme *res;

    if (ld->curDictId == InvalidOid) {
        // Normal mode: process tokens with all dictionaries
        while (ld->towork.head) {
            ParsedLex *curVal = ld->towork.head;
            map = ld->cfg->map + curVal->type;

            // Skip invalid token types
            if (curVal->type == 0 || curVal->type >= ld->cfg->lenmap || map->len == 0) {
                RemoveHead(ld);
                continue;
            }

            // Try each dictionary for this token type
            for (i = ld->posDict; i < map->len; i++) {
                dict = lookup_ts_dictionary_cache(map->dictIds[i]);

                // Call dictionary's lexize function
                ld->dictState.isend = ld->dictState.getnext = false;
                res = (TSLexeme *) DatumGetPointer(FunctionCall4(&(dict->lexize),
                    PointerGetDatum(dict->dictData),
                    PointerGetDatum(curVal->lemm),
                    Int32GetDatum(curVal->lenlemm),
                    PointerGetDatum(&ld->dictState)));

                // Dictionary wants more words - switch to multi-word mode
                if (ld->dictState.getnext) {
                    ld->curDictId = DatumGetObjectId(map->dictIds[i]);
                    ld->posDict = i + 1;
                    ld->curSub = curVal->next;
                    if (res)
                        setNewTmpRes(ld, curVal, res);
                    return LexizeExec(ld, correspondLexem);
                }

                if (!res) continue; // Dictionary doesn't know this lexeme

                // Handle filter lexemes (dictionary transforms input)
                if (res->flags & TSL_FILTER) {
                    curVal->lemm = res->lexeme;
                    curVal->lenlemm = strlen(res->lexeme);
                    continue;
                }

                // Found result
                RemoveHead(ld);
                setCorrLex(ld, correspondLexem);
                return res;
            }
            RemoveHead(ld);
        }
    }
    else {
        // Multi-word mode: specific dictionary wants additional words
        dict = lookup_ts_dictionary_cache(ld->curDictId);

        while (ld->curSub) {
            ParsedLex *curVal = ld->curSub;
            map = ld->cfg->map + curVal->type;

            // Check if dictionary can handle this token type
            if (curVal->type != 0) {
                bool dictExists = false;
                if (curVal->type >= ld->cfg->lenmap || map->len == 0) {
                    ld->curSub = curVal->next;
                    continue;
                }

                for (i = 0; i < map->len && !dictExists; i++)
                    if (ld->curDictId == DatumGetObjectId(map->dictIds[i]))
                        dictExists = true;

                if (!dictExists) {
                    // Dictionary can't handle this type, return to normal mode
                    ld->curDictId = InvalidOid;
                    return LexizeExec(ld, correspondLexem);
                }
            }

            // Process with current dictionary
            ld->dictState.isend = (curVal->type == 0);
            ld->dictState.getnext = false;
            res = (TSLexeme *) DatumGetPointer(FunctionCall4(&(dict->lexize),
                PointerGetDatum(dict->dictData),
                PointerGetDatum(curVal->lemm),
                Int32GetDatum(curVal->lenlemm),
                PointerGetDatum(&ld->dictState)));

            if (ld->dictState.getnext) {
                // Dictionary wants more words
                ld->curSub = curVal->next;
                if (res)
                    setNewTmpRes(ld, curVal, res);
                continue;
            }

            if (res || ld->tmpRes) {
                // Dictionary finished - clean up and return result
                if (res) {
                    moveToWaste(ld, ld->curSub);
                } else {
                    res = ld->tmpRes;
                    moveToWaste(ld, ld->lastRes);
                }

                // Reset state and return
                ld->curDictId = InvalidOid;
                ld->posDict = 0;
                ld->lastRes = NULL;
                ld->tmpRes = NULL;
                setCorrLex(ld, correspondLexem);
                return res;
            }

            // Dictionary failed - return to normal mode
            ld->curDictId = InvalidOid;
            return LexizeExec(ld, correspondLexem);
        }
    }

    setCorrLex(ld, correspondLexem);
    return NULL;
}
```