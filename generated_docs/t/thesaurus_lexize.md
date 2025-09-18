# thesaurus_lexize

## Location
src/backend/tsearch/dict_thesaurus.c: 788 - 879

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
  - checkMatch
  - lookup_ts_dictionary_cache
  - FunctionCall4
  - findTheLexeme
  - findVariant
- Types referenced:
  - DictThesaurus
  - DictSubState
  - TSLexeme
  - LexemeInfo
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