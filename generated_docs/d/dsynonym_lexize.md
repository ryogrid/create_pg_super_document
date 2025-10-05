# dsynonym_lexize

## Location
[src/backend/tsearch/dict_synonym.c:210-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_synonym.c#L210-L241)

## Overview
Performs synonym replacement by searching for input tokens in the initialized synonym dictionary and returning appropriate lexeme replacements.

## Definition

```c
Datum
dsynonym_lexize(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the core lexicalization routine for PostgreSQL's synonym dictionary. It takes an input token and searches the pre-built sorted synonym array to find matching entries. When a match is found, it returns the corresponding synonym as a TSLexeme array.

The function performs these operations:
1. Extracts the input token and its length from function arguments
2. Creates a search key, applying case conversion if necessary
3. Performs binary search using bsearch() and compareSyn() for efficient lookup
4. Returns a TSLexeme array containing the synonym replacement if found
5. Returns NULL if no matching synonym exists

The function respects the case sensitivity setting established during dictionary initialization.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - d: Pointer to initialized DictSyn structure
  - in: Input token to look up
  - len: Length of input token

## Dependencies
- Functions called/Symbols referenced:
  - DictSyn, Syn, TSLexeme (data structures)
  - [pnstrdup](../p/pnstrdup.md) (string duplication with length)
  - [lowerstr_with_len](../l/lowerstr_with_len.md) (case conversion)
  - bsearch with compareSyn (binary search)
  - [palloc0](../p/palloc0.md) (memory allocation)
- Called from (representative examples):
  - PostgreSQL text search lexicalization system (no direct callers in provided data)

## Notes and Other Information
- This is a PostgreSQL function callable during text search processing
- Uses binary search for O(log n) lookup performance on large synonym dictionaries
- Handles case sensitivity by converting search keys to lowercase when appropriate
- Returns a TSLexeme array with exactly one synonym entry plus a NULL terminator
- Preserves prefix flags from the original synonym definition
- Protects against Solaris bsearch bug by checking array length before searching

## Simplified Source

```c
Datum
dsynonym_lexize(PG_FUNCTION_ARGS)
{
    DictSyn *d = (DictSyn *) PG_GETARG_POINTER(0);
    char *in = (char *) PG_GETARG_POINTER(1);
    int32 len = PG_GETARG_INT32(2);
    Syn key, *found;
    TSLexeme *res;

    // Handle empty input or empty dictionary
    if (len <= 0 || d->len <= 0)
        PG_RETURN_POINTER(NULL);

    // Create search key with proper case handling
    if (d->case_sensitive)
        key.in = pnstrdup(in, len);
    else
        key.in = lowerstr_with_len(in, len);
    key.out = NULL;

    // Binary search for synonym match
    found = (Syn *) bsearch(&key, d->syn, d->len, sizeof(Syn), compareSyn);
    pfree(key.in);

    if (!found)
        PG_RETURN_POINTER(NULL);

    // Return synonym as TSLexeme array
    res = palloc0(sizeof(TSLexeme) * 2);
    res[0].lexeme = pnstrdup(found->out, found->outlen);
    res[0].flags = found->flags;
    // res[1] is NULL-initialized by palloc0

    PG_RETURN_POINTER(res);
}
```