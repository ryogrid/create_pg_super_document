# dsimple_lexize

## Location
[src/backend/tsearch/dict_simple.c:75-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_simple.c#L75-L105)

## Overview
Performs lexical analysis on input text using a simple dictionary by converting text to lowercase, checking against stopwords, and returning appropriate lexemes based on dictionary configuration.

## Definition

```c
Datum
dsimple_lexize(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the core lexical analysis routine for PostgreSQL's simple dictionary. It takes an input word, converts it to lowercase, and applies the dictionary's rules to determine the appropriate lexical output. The function implements a three-stage decision process: first checking if the word is empty or matches a stopword (in which case it returns an empty lexeme array to indicate rejection), then checking if the dictionary is configured to accept words (returning the lowercase word as a lexeme), or finally reporting the word as unrecognized by returning NULL. This function is essential for text search operations, as it determines which words are indexed and how they are normalized.

## Parameters / Member Variables
- : A DictSimple pointer containing the dictionary configuration (stoplist and accept flag)
- : A char pointer to the input text to be processed
- : An int32 specifying the length of the input text

## Dependencies
- Functions called/Symbols referenced:
  - DictSimple (structure type for dictionary state)
  - TSLexeme (structure type for lexical output)
  - [lowerstr_with_len](../l/lowerstr_with_len.md) (converts text to lowercase with specified length)
  - [searchstoplist](../s/searchstoplist.md) (checks if word exists in stopword list)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation with zero initialization)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager)

## Notes and Other Information
- Returns an array of TSLexeme structures with at least 2 elements (the last being NULL-terminated)
- Implements three possible outcomes: stopword rejection (empty array), word acceptance (single lexeme), or unrecognized word (NULL return)
- The function always creates lowercase versions of input text for consistent processing
- Memory management includes proper cleanup with pfree() for rejected words
- Part of PostgreSQL's text search dictionary framework
- Located in src/backend/tsearch/dict_simple.c:75-105

## Simplified Source

```c
Datum dsimple_lexize(PG_FUNCTION_ARGS) {
    // Extract arguments
    DictSimple *d = (DictSimple *) PG_GETARG_POINTER(0);
    char *in = (char *) PG_GETARG_POINTER(1);
    int32 len = PG_GETARG_INT32(2);

    // Convert input to lowercase
    char *txt = lowerstr_with_len(in, len);

    // Check if empty or stopword
    if (*txt == '\0' || searchstoplist(&(d->stoplist), txt)) {
        // Reject as stopword - return empty lexeme array
        pfree(txt);
        TSLexeme *res = palloc0(sizeof(TSLexeme) * 2);
        PG_RETURN_POINTER(res);
    }
    else if (d->accept) {
        // Accept word - return it as lexeme
        TSLexeme *res = palloc0(sizeof(TSLexeme) * 2);
        res[0].lexeme = txt;
        PG_RETURN_POINTER(res);
    }
    else {
        // Dictionary doesn't accept - report as unrecognized
        pfree(txt);
        PG_RETURN_POINTER(NULL);
    }
}
```