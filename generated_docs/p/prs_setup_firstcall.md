# prs_setup_firstcall

## Location
[src/backend/tsearch/wparser.c:162-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L162-L215)

## Overview
Initializes function context and performs text parsing during the first call of PostgreSQL text search parser functions that tokenize input text.

## Definition

```c
static void
prs_setup_firstcall(FuncCallContext *funcctx, FunctionCallInfo fcinfo,
					Oid prsid, text *txt)
```
## Detailed Description
This function sets up the necessary data structures and performs the complete text parsing process for PostgreSQL text search parser functions during their first call. Unlike  which retrieves token type definitions, this function actually parses input text and stores all resulting lexemes (tokens) for subsequent retrieval. The function uses the parser's start, token, and end methods to process the entire input text and stores each lexeme with its type information.

The function performs these key operations:
1. Initializes a PrsStorage structure to hold parsing results
2. Calls the parser's start method to initialize parsing of the input text
3. Repeatedly calls the parser's token method to extract lexemes until parsing is complete
4. Dynamically resizes the lexeme storage array as needed
5. Calls the parser's end method to finalize parsing
6. Sets up tuple descriptor and attribute metadata for the return type

The parsing process extracts all tokens from the input text in a single pass during the first call, storing them in memory for efficient retrieval in subsequent function calls.

## Parameters / Member Variables
- `*funcctx`: Function call context structure used for multi-call functions
- `fcinfo`: Function call information containing metadata about the function call
- `prsid`: OID of the text search parser to use for parsing
- `*txt`: Input text to be parsed and tokenized
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md)
  - FunctionCall2 (parser start method)
  - FunctionCall3 (parser token method)
  - FunctionCall1 (parser end method)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [repalloc](../r/repalloc.md)
  - [get_call_result_type](../g/get_call_result_type.md)
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](palloc.md)
  - memcpy
- Called from (representative examples):
  - [ts_parse_byid](../t/ts_parse_byid.md)
  - [ts_parse_byname](../t/ts_parse_byname.md)

## Notes and Other Information
- This is a static function internal to the wparser.c module
- Performs complete text parsing during the first call, unlike token type functions that just retrieve metadata
- Dynamically resizes the lexeme storage array, starting with 16 entries and doubling as needed
- Uses PostgreSQL's multi-call function framework for returning sets of rows
- Memory allocation is done in the multi-call memory context to ensure persistence across calls
- Each lexeme is stored with its text content and token type for later retrieval
- The function properly manages parser lifecycle by calling start, token (repeatedly), and end methods
- Handles variable-length input text through VARDATA_ANY and VARSIZE_ANY_EXHDR macros

## Simplified Source

```c
static void prs_setup_firstcall(FuncCallContext *funcctx, FunctionCallInfo fcinfo,
                                Oid prsid, text *txt) {
    TupleDesc tupdesc;
    MemoryContext oldcontext;
    PrsStorage *st;
    TSParserCacheEntry *prs = lookup_ts_parser_cache(prsid);
    char *lex = NULL;
    int llen = 0, type = 0;
    void *prsdata;

    // Switch to persistent memory context
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    // Initialize storage for lexemes (starting with 16 entries)
    st = (PrsStorage *) palloc(sizeof(PrsStorage));
    st->cur = 0;
    st->len = 16;
    st->list = (LexemeEntry *) palloc(sizeof(LexemeEntry) * st->len);

    // Start parsing the input text
    prsdata = (void *) DatumGetPointer(FunctionCall2(&prs->prsstart,
                                                     PointerGetDatum(VARDATA_ANY(txt)),
                                                     Int32GetDatum(VARSIZE_ANY_EXHDR(txt))));

    // Extract all tokens from input text
    while ((type = DatumGetInt32(FunctionCall3(&prs->prstoken,
                                              PointerGetDatum(prsdata),
                                              PointerGetDatum(&lex),
                                              PointerGetDatum(&llen)))) != 0) {
        // Expand storage if needed
        if (st->cur >= st->len) {
            st->len = 2 * st->len;
            st->list = (LexemeEntry *) repalloc(st->list, sizeof(LexemeEntry) * st->len);
        }

        // Store lexeme text and type
        st->list[st->cur].lexeme = palloc(llen + 1);
        memcpy(st->list[st->cur].lexeme, lex, llen);
        st->list[st->cur].lexeme[llen] = '\0';
        st->list[st->cur].type = type;
        st->cur++;
    }

    // End parsing
    FunctionCall1(&prs->prsend, PointerGetDatum(prsdata));

    // Finalize storage and setup function context
    st->len = st->cur;
    st->cur = 0;
    funcctx->user_fctx = (void *) st;

    // Setup tuple descriptor for return type
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    funcctx->tuple_desc = tupdesc;
    funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

    MemoryContextSwitchTo(oldcontext);
}
```