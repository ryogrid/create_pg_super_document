# ts_lexize

## Location
[src/backend/tsearch/dict.c:27-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict.c#L27-L85)

## Overview
This function lexizes a single word using a specified text search dictionary, primarily serving as a debug function for testing dictionary operations in PostgreSQL's full-text search system.

## Definition

```c
struct_array_builtin(da, ptr - res, TEXTOID);
```
## Detailed Description
The  function takes a dictionary OID and input text, then applies the dictionary's lexization process to transform the input word into normalized lexemes. This function is designed primarily for debugging and testing purposes, allowing users to see how a particular dictionary would process a given word.

The function performs the following operations:
1. Retrieves the dictionary cache entry using the provided dictionary OID
2. Calls the dictionary's lexize function to process the input text
3. Handles cases where the dictionary requires multiple calls (using the  mechanism)
4. Converts the resulting TSLexeme array into a PostgreSQL text array for return
5. Properly manages memory by freeing allocated resources

The function supports dictionaries that may require multiple lexization calls, as indicated by the  flag. This allows for complex dictionary processing that may need to maintain state between calls.

## Parameters / Member Variables
-  (Oid): The object identifier of the text search dictionary to use for lexization
-  (text*): The input text/word to be lexized by the dictionary

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves cached dictionary information
  - : Calls the dictionary's lexize function
  - : Constructs PostgreSQL array from lexeme results
  - : Dictionary cache structure
  - : Structure representing lexeme results
  - : State structure for dictionary processing

- Called from (representative examples):
  - No direct references found (primarily used as SQL function)

## Notes and Other Information
- This function is primarily intended for debugging and testing dictionary behavior
- Located in src/backend/tsearch/dict.c:27-85
- Returns NULL if the dictionary produces no lexemes for the input
- Handles memory management carefully, freeing both the lexeme strings and the lexeme array
- Supports dictionaries that require multiple processing passes through the getnext mechanism
- The function converts C-style TSLexeme results into PostgreSQL's text array format for SQL compatibility
- Input text is processed using VARDATA_ANY and VARSIZE_ANY_EXHDR macros to handle varlena data types properly

## Simplified Source

```c
Datum ts_lexize(PG_FUNCTION_ARGS)
{
    Oid dictId = PG_GETARG_OID(0);
    text *in = PG_GETARG_TEXT_PP(1);
    ArrayType *a;
    TSDictionaryCacheEntry *dict;
    TSLexeme *res, *ptr;
    Datum *da;
    DictSubState dstate = {false, false, NULL};

    // Look up dictionary in cache
    dict = lookup_ts_dictionary_cache(dictId);

    // Call dictionary's lexize function
    res = (TSLexeme *) DatumGetPointer(FunctionCall4(&dict->lexize,
                                                     PointerGetDatum(dict->dictData),
                                                     PointerGetDatum(VARDATA_ANY(in)),
                                                     Int32GetDatum(VARSIZE_ANY_EXHDR(in)),
                                                     PointerGetDatum(&dstate)));

    // Handle multi-pass dictionaries if needed
    if (dstate.getnext)
    {
        dstate.isend = true;
        ptr = (TSLexeme *) DatumGetPointer(FunctionCall4(&dict->lexize, /* same args */));
        if (ptr != NULL)
            res = ptr;
    }

    // Return NULL if no lexemes produced
    if (!res)
        PG_RETURN_NULL();

    // Count lexemes and build datum array
    ptr = res;
    while (ptr->lexeme) ptr++;
    da = (Datum *) palloc(sizeof(Datum) * (ptr - res));

    ptr = res;
    while (ptr->lexeme)
    {
        da[ptr - res] = CStringGetTextDatum(ptr->lexeme);
        ptr++;
    }

    // Construct PostgreSQL array from lexemes
    a = construct_array_builtin(da, ptr - res, TEXTOID);

    // Clean up memory
    ptr = res;
    while (ptr->lexeme)
    {
        pfree(DatumGetPointer(da[ptr - res]));
        pfree(ptr->lexeme);
        ptr++;
    }
    pfree(res);
    pfree(da);

    PG_RETURN_POINTER(a);
}
```