# array_to_tsvector

## Location
[src/backend/utils/adt/tsvector_op.c:747-818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L747-L818)

## Overview
Constructs a TSVector from an array of lexeme strings, creating a valid full-text search vector with sorted and deduplicated lexemes.

## Definition
```c
Datum array_to_tsvector(PG_FUNCTION_ARGS)
```

## Detailed Description
This function builds a TSVector from an input array of text elements representing lexemes. It performs several validation and normalization steps to ensure the resulting TSVector meets PostgreSQL's requirements: lexemes must be non-null, non-empty, sorted alphabetically, and contain no duplicates.

The function first validates all input elements, rejecting arrays containing null values or empty strings. It then sorts the lexemes using quicksort and removes duplicates using a unique operation. Finally, it constructs a properly formatted TSVector by calculating the required memory space, allocating the structure, and copying each lexeme into the appropriate position within the TSVector's string storage area.

The resulting TSVector contains lexemes without position or weight information (haspos = 0 for all entries), making it suitable for basic text matching operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Array of text elements to convert into TSVector lexemes

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P - Extract array argument
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) - Decompose array into elements
  - VARSIZE - Get size of variable-length data
  - ereport - Report errors for validation failures
  - qsort - [Sort](../S/Sort.md) lexemes alphabetically
  - [qunique](../q/qunique.md) - Remove duplicate lexemes
  - [compare_text_lexemes](../c/compare_text_lexemes.md) - Comparison function for lexeme sorting
  - CALCDATASIZE - Calculate required TSVector size
  - [palloc0](../p/palloc0.md) - Allocate zero-initialized memory
  - SET_VARSIZE - Set PostgreSQL variable size
  - ARRPTR - Get pointer to WordEntry array
  - STRPTR - Get pointer to string data area
  - VARDATA - Get pointer to variable-length data
  - memcpy - Copy lexeme data
  - PG_FREE_IF_COPY - Free copied arguments if needed
  - PG_RETURN_POINTER - Return result pointer
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- Validates input by rejecting null values and empty strings in the lexeme array
- Automatically sorts lexemes alphabetically and removes duplicates to meet TSVector requirements
- Creates TSVector entries without position information (haspos = 0)
- Calculates exact memory requirements before allocation to avoid waste
- Useful for creating TSVector from manually curated lexeme lists
- Part of PostgreSQL's full-text search functionality for TSVector construction
- Complement to tsvector_to_array function for round-trip conversion
- Generated TSVectors are suitable for basic text search but lack positional information for phrase searches

## Simplified Source

```c
Datum array_to_tsvector(PG_FUNCTION_ARGS) {
    ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
    Datum *dlexemes;
    bool *nulls;
    int nitems;

    // Extract array elements
    deconstruct_array_builtin(v, TEXTOID, &dlexemes, &nulls, &nitems);

    // Validate: reject nulls and empty strings
    for (int i = 0; i < nitems; i++) {
        if (nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("lexeme array may not contain nulls")));

        if (VARSIZE(dlexemes[i]) - VARHDRSZ == 0)
            ereport(ERROR, (errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING),
                           errmsg("lexeme array may not contain empty strings")));
    }

    // Sort and deduplicate lexemes
    if (nitems > 1) {
        qsort(dlexemes, nitems, sizeof(Datum), compare_text_lexemes);
        nitems = qunique(dlexemes, nitems, sizeof(Datum), compare_text_lexemes);
    }

    // Calculate storage space needed
    int datalen = 0;
    for (int i = 0; i < nitems; i++)
        datalen += VARSIZE(dlexemes[i]) - VARHDRSZ;
    int tslen = CALCDATASIZE(nitems, datalen);

    // Create TSVector structure
    TSVector tsout = (TSVector) palloc0(tslen);
    SET_VARSIZE(tsout, tslen);
    tsout->size = nitems;

    // Fill in lexeme data
    WordEntry *arrout = ARRPTR(tsout);
    char *cur = STRPTR(tsout);
    for (int i = 0; i < nitems; i++) {
        char *lex = VARDATA(dlexemes[i]);
        int lex_len = VARSIZE(dlexemes[i]) - VARHDRSZ;

        memcpy(cur, lex, lex_len);
        arrout[i].haspos = 0;  // No position info
        arrout[i].len = lex_len;
        arrout[i].pos = cur - STRPTR(tsout);
        cur += lex_len;
    }

    PG_FREE_IF_COPY(v, 0);
    PG_RETURN_POINTER(tsout);
}
```