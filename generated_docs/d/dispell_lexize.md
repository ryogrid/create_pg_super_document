# dispell_lexize

## Location
src/backend/tsearch/dict_ispell.c: 111 - 148

## Overview
Performs lexical analysis on input text using an Ispell dictionary to normalize words and filter out stopwords.

## Definition
```c
Datum dispell_lexize(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dispell_lexize` function is the core lexical analysis routine for PostgreSQL's Ispell text search dictionary. It takes an input word and processes it through the following steps:

1. **Input validation**: Checks if the input length is valid (> 0)
2. **Case normalization**: Converts the input text to lowercase using `lowerstr_with_len`
3. **Morphological analysis**: Uses `NINormalizeWord` to find base forms and variations of the input word based on dictionary and affix rules
4. **Stopword filtering**: Removes any results that appear in the configured stopword list
5. **Result compaction**: Compacts the result array by removing filtered entries

The function returns an array of `TSLexeme` structures containing the normalized forms of the input word, or NULL if no valid lexemes are found or if all results are filtered as stopwords.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `d`: `DictISpell` pointer to the initialized dictionary object
  - `in`: `char` pointer to the input text to be analyzed
  - `len`: `int32` length of the input text

## Dependencies
- Functions called/Symbols referenced:
  - [lowerstr_with_len](../l/lowerstr_with_len.md): Converts input text to lowercase with specified length
  - `NINormalizeWord`: Performs morphological normalization using Ispell rules
  - [searchstoplist](../s/searchstoplist.md): Checks if a word exists in the stopword list
  - [pfree](../p/pfree.md): Frees allocated memory for filtered lexemes
  - `memcpy`: Copies lexeme structures during array compaction
  - `DictISpell`: Dictionary structure type
  - `TSLexeme`: Text search lexeme structure type
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL function call mechanism)

## Notes and Other Information
- This function is designed to be called through PostgreSQL's function call interface as part of text search operations
- Returns NULL immediately if input length is zero or negative
- The function handles memory management by freeing lexemes that are filtered as stopwords
- [Result](../R/Result.md) array compaction ensures no gaps remain after stopword filtering
- The returned `TSLexeme` array is null-terminated (final entry has `lexeme = NULL`)
- Input text is processed in a case-insensitive manner through lowercase conversion
- The function integrates with PostgreSQL's text search framework for full-text indexing and querying