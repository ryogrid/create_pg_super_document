# thesaurus_lexize

## Location
[src/backend/tsearch/dict_thesaurus.c:788-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L788-L879)

## Overview
Main PostgreSQL function that implements thesaurus dictionary lexeme processing, transforming input words through pattern matching and substitution rules.

## Definition
```c
Datum thesaurus_lexize(PG_FUNCTION_ARGS)
```

## Detailed Description
The `thesaurus_lexize` function is the core implementation of PostgreSQL's thesaurus dictionary functionality. It processes input lexemes by first passing them through a subdictionary for normalization, then attempting to match the normalized results against thesaurus patterns for substitution. The function implements a sophisticated matching algorithm that handles multi-lexeme patterns, variants, and stop-words. It maintains state information to support incremental processing and backtracking when multiple pattern matches are possible. The function follows PostgreSQL's function call convention using `PG_FUNCTION_ARGS` and returns `Datum` values.

## Parameters / Member Variables
The function receives its parameters through PostgreSQL's standard function argument mechanism:
- `PG_GETARG_POINTER(0)`: DictThesaurus dictionary data structure
- `PG_GETARG_DATUM(1)`: Input word/lexeme to be processed
- `PG_GETARG_DATUM(2)`: Input length or additional lexeme information
- `PG_GETARG_POINTER(3)`: DictSubState for maintaining processing state between calls

## Dependencies
- Functions called/Symbols referenced:
  - [checkMatch](../c/checkMatch.md)
  - [lookup_ts_dictionary_cache](../l/lookup_ts_dictionary_cache.md)
  - FunctionCall4
  - [findTheLexeme](../f/findTheLexeme.md)
  - [findVariant](../f/findVariant.md)
- Types referenced:
  - DictThesaurus
  - [DictSubState](../D/DictSubState.md)
  - TSLexeme
  - [LexemeInfo](../L/LexemeInfo.md)
- Called from (representative examples):
  - PostgreSQL text search system (via function call infrastructure)

## Notes and Other Information
- This function implements the PostgreSQL dictionary interface and must conform to its calling conventions
- Supports nested dictionary processing through subdictionary delegation
- Handles three types of input: recognized words (with lexemes), stop-words (no lexemes), and unrecognized words (null)
- Uses sophisticated state management to support backtracking when multiple thesaurus patterns match
- The function validates that exactly 4 arguments are provided and prevents nested calls for security
- Memory management uses PostgreSQL's palloc/pfree system
- Returns NULL when no substitution is found or processing is complete
- The `getnext` flag in `dstate` controls whether the caller should expect more results

## Simplified Source

```c
// Simplified version of thesaurus_lexize
Datum thesaurus_lexize(PG_FUNCTION_ARGS) {
    // Extract function arguments
    DictThesaurus *dict = (DictThesaurus *) PG_GETARG_POINTER(0);
    DictSubState *state = (DictSubState *) PG_GETARG_POINTER(3);

    TSLexeme *subdictResult = NULL;
    LexemeInfo *storedInfo = NULL;
    LexemeInfo *currentInfo = NULL;
    uint16 currentPos = 0;
    bool hasMoreResults = false;

    // Step 1: Validate function call and state
    if (PG_NARGS() != 4 || state == NULL) {
        elog(ERROR, "forbidden call of thesaurus or nested call");
    }

    if (state->isend) {
        PG_RETURN_POINTER(NULL);  // Processing complete
    }

    // Step 2: Initialize position from previous state
    storedInfo = (LexemeInfo *) state->private_state;
    if (storedInfo) {
        currentPos = storedInfo->posinsubst + 1;
    }

    // Step 3: Ensure subdictionary is valid and call it
    if (!dict->subdict->isvalid) {
        dict->subdict = lookup_ts_dictionary_cache(dict->subdictOid);
    }

    // Call the subdictionary to normalize the input
    subdictResult = (TSLexeme *) DatumGetPointer(
        FunctionCall4(&(dict->subdict->lexize),
                     PointerGetDatum(dict->subdict->dictData),
                     PG_GETARG_DATUM(1),     // input word
                     PG_GETARG_DATUM(2),     // input length
                     PointerGetDatum(NULL))); // no nested state

    // Step 4: Process subdictionary result
    if (subdictResult && subdictResult->lexeme) {
        // Case A: Subdictionary produced lexemes - try thesaurus matching
        TSLexeme *currentLexeme = subdictResult;

        // Process each variant group from the subdictionary
        while (currentLexeme->lexeme) {
            uint16 variantNum = currentLexeme->nvariant;
            uint16 lexemeCount = 0;
            TSLexeme *variantStart = currentLexeme;
            LexemeInfo **lexemeInfos;

            // Count lexemes in this variant group
            while (currentLexeme->lexeme && currentLexeme->nvariant == variantNum) {
                lexemeCount++;
                currentLexeme++;
            }

            // Look up thesaurus entries for all lexemes in this variant
            lexemeInfos = (LexemeInfo **) palloc(sizeof(LexemeInfo *) * lexemeCount);
            bool allFound = true;

            for (uint16 i = 0; i < lexemeCount; i++) {
                lexemeInfos[i] = findTheLexeme(dict, variantStart[i].lexeme);
                if (lexemeInfos[i] == NULL) {
                    allFound = false;
                    break;
                }
            }

            if (allFound) {
                // All lexemes found in thesaurus - try to find variant match
                currentInfo = findVariant(currentInfo, storedInfo, currentPos,
                                        lexemeInfos, lexemeCount);
            }

            pfree(lexemeInfos);
        }
    }
    else if (subdictResult) {
        // Case B: Subdictionary returned empty result (stop-word)
        LexemeInfo *stopWordInfo = findTheLexeme(dict, NULL);
        currentInfo = findVariant(NULL, storedInfo, currentPos, &stopWordInfo, 1);
    }
    else {
        // Case C: Subdictionary didn't recognize the word
        currentInfo = NULL;
    }

    // Step 5: Update state and check for matches
    state->private_state = (void *) currentInfo;

    if (!currentInfo) {
        // No thesaurus match found
        state->getnext = false;
        PG_RETURN_POINTER(NULL);
    }

    // Step 6: Check if we have a valid substitution
    TSLexeme *result = checkMatch(dict, currentInfo, currentPos, &hasMoreResults);

    if (result != NULL) {
        // Found a match - return it
        state->getnext = hasMoreResults;
        PG_RETURN_POINTER(result);
    }

    // Step 7: No match this time, but continue processing
    state->getnext = true;
    PG_RETURN_POINTER(NULL);
}
```

Key simplifications made:
- Added clear step-by-step processing phases with comments
- Simplified variable names for better readability
- Consolidated complex variant processing logic into clearer flow
- Made the three main processing cases (lexemes, stop-words, unrecognized) more explicit
- Reduced nested conditions and improved control flow
- Added descriptive comments explaining the thesaurus matching algorithm
- Preserved all essential functionality while making the logic more accessible
- Focused on the main algorithm rather than low-level memory management details