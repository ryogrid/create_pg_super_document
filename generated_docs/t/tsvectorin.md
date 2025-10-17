# tsvectorin

## Location
[src/backend/utils/adt/tsvector.c:175-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L175-L313)

## Overview
PostgreSQL input function that parses a string representation of a tsvector and converts it into the internal TSVector data structure.

## Definition
```c
Datum tsvectorin(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the primary input conversion routine for tsvector data type in PostgreSQL. It parses a text string containing words and optional positional information, validates the input against various limits, and constructs the internal TSVector representation. The function handles dynamic memory allocation for both the word entries array and the temporary string buffer, expanding them as needed during parsing. It processes each token through the tsvector parser, collecting words and their positions, then uses uniqueentry to eliminate duplicates and merge position information. Finally, it constructs the compact TSVector structure with proper alignment and memory layout required for efficient storage and retrieval.

## Parameters / Member Variables
- Function follows PostgreSQL's V1 calling convention, receiving arguments through PG_FUNCTION_ARGS macro:

## Dependencies
- Functions called/Symbols referenced:
  - [init_tsvector_parser](../i/init_tsvector_parser.md) (initialize parser state)
  - [gettoken_tsvector](../g/gettoken_tsvector.md) (extract tokens from input)
  - [close_tsvector_parser](../c/close_tsvector_parser.md) (cleanup parser resources)
  - [uniqueentry](../u/uniqueentry.md) (remove duplicates and merge positions)
  - [palloc](../p/palloc.md)/palloc0 (PostgreSQL memory allocation)
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - ereturn (error return with context)
  - SOFT_ERROR_OCCURRED (check for parsing errors)
  - CALCDATASIZE (calculate total TSVector size)
  - SET_VARSIZE (set PostgreSQL variable-length header)
  - ARRPTR/STRPTR (access TSVector components)
  - SHORTALIGN (ensure proper memory alignment)
  - PG_RETURN_TSVECTOR (return TSVector result)
- Called from:
  - PostgreSQL type system (no direct references found in symbol analysis)

## Notes and Other Information
- Enforces multiple limits: MAXSTRLEN for individual words, MAXSTRPOS for total string length, MAXNUMPOS for position arrays
- Uses dynamic buffer expansion strategy starting with 256 bytes for temporary storage and 64 entries for word array
- Implements comprehensive error handling with soft error reporting through escontext
- Constructs memory-efficient TSVector layout with string data and positional information properly aligned
- Critical entry point for converting text input into PostgreSQL's full-text search data structures
- The function maintains referential integrity between word entries and their string/position data throughout the conversion process

## Simplified Source

```c
Datum
tsvectorin(PG_FUNCTION_ARGS)
{
    char *buf = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    TSVectorParseState state;
    WordEntryIN *arr;
    int totallen, arrlen = 64, len = 0;
    TSVector in;
    char *token, *tmpbuf, *cur;
    int toklen, buflen = 256;
    WordEntryPos *pos;
    int poslen;

    // Initialize parser and allocate initial buffers
    state = init_tsvector_parser(buf, 0, escontext);
    arr = palloc(sizeof(WordEntryIN) * arrlen);
    cur = tmpbuf = palloc(buflen);

    // Parse tokens from input string
    while (gettoken_tsvector(state, &token, &toklen, &pos, &poslen, NULL)) {
        // Validate token and buffer limits
        if (toklen >= MAXSTRLEN)
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("word is too long (%ld bytes, max %ld bytes)",
                            (long) toklen, (long) (MAXSTRLEN - 1))));

        if (cur - tmpbuf > MAXSTRPOS)
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("string is too long for tsvector")));

        // Expand buffers if needed
        if (len >= arrlen) {
            arrlen *= 2;
            arr = repalloc(arr, sizeof(WordEntryIN) * arrlen);
        }
        while ((cur - tmpbuf) + toklen >= buflen) {
            int dist = cur - tmpbuf;
            buflen *= 2;
            tmpbuf = repalloc(tmpbuf, buflen);
            cur = tmpbuf + dist;
        }

        // Store token data
        arr[len].entry.len = toklen;
        arr[len].entry.pos = cur - tmpbuf;
        memcpy(cur, token, toklen);
        cur += toklen;

        // Store position information
        if (poslen != 0) {
            arr[len].entry.haspos = 1;
            arr[len].pos = pos;
            arr[len].poslen = poslen;
        } else {
            arr[len].entry.haspos = 0;
            arr[len].pos = NULL;
            arr[len].poslen = 0;
        }
        len++;
    }

    close_tsvector_parser(state);

    if (SOFT_ERROR_OCCURRED(escontext))
        PG_RETURN_NULL();

    // Remove duplicates and calculate final buffer size
    if (len > 0)
        len = uniqueentry(arr, len, tmpbuf, &buflen);
    else
        buflen = 0;

    if (buflen > MAXSTRPOS)
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("string is too long for tsvector")));

    // Construct final TSVector structure
    totallen = CALCDATASIZE(len, buflen);
    in = palloc0(totallen);
    SET_VARSIZE(in, totallen);
    in->size = len;

    WordEntry *inarr = ARRPTR(in);
    char *strbuf = STRPTR(in);
    int stroff = 0;

    // Copy data into final structure
    for (int i = 0; i < len; i++) {
        memcpy(strbuf + stroff, &tmpbuf[arr[i].entry.pos], arr[i].entry.len);
        arr[i].entry.pos = stroff;
        stroff += arr[i].entry.len;

        if (arr[i].entry.haspos) {
            if (arr[i].poslen > 0xFFFF)
                elog(ERROR, "positions array too long");

            // Copy position count and position data
            stroff = SHORTALIGN(stroff);
            *(uint16 *) (strbuf + stroff) = (uint16) arr[i].poslen;
            stroff += sizeof(uint16);

            memcpy(strbuf + stroff, arr[i].pos, arr[i].poslen * sizeof(WordEntryPos));
            stroff += arr[i].poslen * sizeof(WordEntryPos);

            pfree(arr[i].pos);
        }
        inarr[i] = arr[i].entry;
    }

    Assert((strbuf + stroff - (char *) in) == totallen);

    PG_RETURN_TSVECTOR(in);
}
```