# ts_headline_byid_opt

## Location
[src/backend/tsearch/wparser.c:288-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L288-L338)

## Overview
A PostgreSQL function that generates highlighted headlines from text based on a text search query, using a specified text search configuration by OID and optional formatting parameters.

## Definition
```c
Datum ts_headline_byid_opt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the core implementation for PostgreSQL's text search headline generation functionality. It takes a text search configuration OID, input text, a TSQuery, and optional formatting parameters to produce a highlighted version of the text where query matches are marked up.

The function performs several key operations:
1. Looks up the text search configuration and associated parser in the cache
2. Verifies that the parser supports headline creation
3. Parses the input text using hlparsetext to identify word positions and query matches
4. Deserializes optional formatting parameters
5. Calls the parser's headline function to apply formatting logic
6. Generates the final headline text with markup

The function handles memory management carefully, allocating structures for parsed text and cleaning up all allocated memory before returning.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `PG_GETARG_OID(0)`: The OID of the text search configuration to use
  - `PG_GETARG_TEXT_PP(1)`: The input text to generate headlines from
  - `PG_GETARG_TSQUERY(2)`: The text search query for highlighting
  - `PG_GETARG_TEXT_PP(3)`: Optional formatting parameters (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - TSQuery (text search query type)
  - PG_GETARG_TSQUERY (macro to extract TSQuery argument)
  - PG_NARGS (macro to get argument count)
  - [HeadlineParsedText](../H/HeadlineParsedText.md) (structure for parsed text data)
  - [TSConfigCacheEntry](../T/TSConfigCacheEntry.md) (cached configuration information)
  - [TSParserCacheEntry](../T/TSParserCacheEntry.md) (cached parser information)
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md) (retrieves configuration from cache)
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md) (retrieves parser from cache)
  - [HeadlineWordEntry](../H/HeadlineWordEntry.md) (structure for individual word entries)
  - [hlparsetext](../h/hlparsetext.md) (parses text and identifies query matches)
  - deserialize_deflist (converts options text to parameter list)
  - FunctionCall3 (calls the parser's headline function)
  - [generateHeadline](../g/generateHeadline.md) (generates final headline text)
  - PG_FREE_IF_COPY (memory management for varlena types)
  - [palloc](../p/palloc.md)/pfree (PostgreSQL memory allocation/deallocation)

- Called from (representative examples):
  - [ts_headline_byid](ts_headline_byid.md) (at src/backend/tsearch/wparser.c:341)
  - [ts_headline](ts_headline.md) (at src/backend/tsearch/wparser.c:350)
  - [ts_headline_opt](ts_headline_opt.md) (at src/backend/tsearch/wparser.c:359)

## Notes and Other Information
- This function is the core implementation used by other headline functions
- Supports optional formatting parameters for customizing highlight markup
- Performs validation to ensure the parser supports headline creation
- Uses caching system for efficient configuration and parser lookup
- Manages complex memory allocation for parsed text structures
- The HeadlineParsedText structure is initialized with a default capacity of 32 words
- Returns an error if the specified parser doesn't support headline functionality
- Properly handles both required and optional function arguments

## Simplified Source

```c
Datum ts_headline_byid_opt(PG_FUNCTION_ARGS) {
    Oid tsconfig = PG_GETARG_OID(0);
    text *in = PG_GETARG_TEXT_PP(1);
    TSQuery query = PG_GETARG_TSQUERY(2);
    text *opt = (PG_NARGS() > 3 && PG_GETARG_POINTER(3)) ? PG_GETARG_TEXT_PP(3) : NULL;

    HeadlineParsedText prs;
    List *prsoptions;
    text *out;
    TSConfigCacheEntry *cfg;
    TSParserCacheEntry *prsobj;

    // Look up configuration and parser
    cfg = lookup_ts_config_cache(tsconfig);
    prsobj = lookup_ts_parser_cache(cfg->prsId);

    // Verify parser supports headlines
    if (!OidIsValid(prsobj->headlineOid))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("text search parser does not support headline creation")));

    // Initialize parsed text structure
    memset(&prs, 0, sizeof(HeadlineParsedText));
    prs.lenwords = 32;
    prs.words = (HeadlineWordEntry *) palloc(sizeof(HeadlineWordEntry) * prs.lenwords);

    // Parse input text and identify query matches
    hlparsetext(cfg->cfgId, &prs, query,
                VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in));

    // Process optional formatting parameters
    if (opt)
        prsoptions = deserialize_deflist(PointerGetDatum(opt));
    else
        prsoptions = NIL;

    // Generate headline using parser's headline function
    FunctionCall3(&(prsobj->prsheadline),
                  PointerGetDatum(&prs),
                  PointerGetDatum(prsoptions),
                  PointerGetDatum(query));

    out = generateHeadline(&prs);

    // Clean up memory
    PG_FREE_IF_COPY(in, 1);
    PG_FREE_IF_COPY(query, 2);
    if (opt)
        PG_FREE_IF_COPY(opt, 3);
    pfree(prs.words);
    pfree(prs.startsel);
    pfree(prs.stopsel);

    PG_RETURN_POINTER(out);
}
```