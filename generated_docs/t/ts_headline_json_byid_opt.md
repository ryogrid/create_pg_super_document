# ts_headline_json_byid_opt

## Location
[src/backend/tsearch/wparser.c:443-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L443-L490)

## Overview
A core PostgreSQL function that generates highlighted headlines from JSON documents based on a specified text search configuration and query, with support for customizable headline generation options.

## Definition
```c
Datum ts_headline_json_byid_opt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs the core logic for generating highlighted headlines from JSON documents. It takes a text search configuration ID, a JSON document, a TSQuery, and optional headline parameters. The function processes JSON string values by transforming them through text search parsing and highlighting matching terms. It uses a specialized JSON transformation approach that applies headline generation to string values within the JSON structure while preserving the overall JSON format.

The function initializes parsing structures, looks up the appropriate text search configuration and parser, processes headline options, and then transforms the JSON document using `transform_json_string_values` with a `headline_json_value` action. It includes comprehensive error handling for unsupported parsers and memory management for temporary structures.

## Parameters / Member Variables
- `tsconfig` (Oid): Text search configuration ID to use for headline generation
- `json` (text*): Input JSON document containing text to be processed
- `query` (TSQuery): Text search query specifying terms to highlight
- `opt` (text*): Optional parameter containing headline generation options (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [headline_json_value](../h/headline_json_value.md): Action function for processing individual JSON string values
  - [transform_json_string_values](transform_json_string_values.md): Core JSON transformation function
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md): Retrieves cached text search configuration
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md): Retrieves cached text search parser
  - `deserialize_deflist`: Parses headline options from text format
  - [palloc](../p/palloc.md): PostgreSQL memory allocation
  - [palloc0](../p/palloc0.md): PostgreSQL zero-initialized memory allocation
  - [pfree](../p/pfree.md): PostgreSQL memory deallocation
  - Various PostgreSQL macros: `PG_GETARG_*`, `PG_FREE_IF_COPY`, `PG_RETURN_TEXT_P`
- Called from (representative examples):
  - [ts_headline_json](ts_headline_json.md): Wrapper using current default configuration
  - [ts_headline_json_byid](ts_headline_json_byid.md): Wrapper without options parameter
  - [ts_headline_json_opt](ts_headline_json_opt.md): Wrapper using current default configuration with options

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c at lines 443-490
- This is the main implementation function for JSON headline generation functionality
- Includes error checking for parsers that do not support headline creation
- Uses a sophisticated JSON transformation approach that preserves JSON structure while highlighting text content
- Manages complex memory allocation patterns including dynamic word entry arrays
- The function supports optional headline generation parameters through the `opt` parameter
- Part of PostgreSQL full-text search functionality specifically designed for JSON document processing

## Simplified Source

```c
Datum
ts_headline_json_byid_opt(PG_FUNCTION_ARGS)
{
    // Extract arguments: config ID, JSON text, query, and optional options
    Oid tsconfig = PG_GETARG_OID(0);
    text *json = PG_GETARG_TEXT_P(1);
    TSQuery query = PG_GETARG_TSQUERY(2);
    text *opt = (PG_NARGS() > 3 && PG_GETARG_POINTER(3)) ? PG_GETARG_TEXT_P(3) : NULL;

    // Initialize headline parsing structures
    HeadlineParsedText prs;
    HeadlineJsonState *state = palloc0(sizeof(HeadlineJsonState));

    // Set up word array for parsing (initial size: 32 words)
    memset(&prs, 0, sizeof(HeadlineParsedText));
    prs.lenwords = 32;
    prs.words = (HeadlineWordEntry *) palloc(sizeof(HeadlineWordEntry) * prs.lenwords);

    // Configure state with text search config and parser
    state->prs = &prs;
    state->cfg = lookup_ts_config_cache(tsconfig);
    state->prsobj = lookup_ts_parser_cache(state->cfg->prsId);
    state->query = query;
    state->prsoptions = opt ? deserialize_deflist(PointerGetDatum(opt)) : NIL;

    // Verify parser supports headline generation
    if (!OidIsValid(state->prsobj->headlineOid))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("text search parser does not support headline creation")));

    // Transform JSON by applying headlines to string values
    text *out = transform_json_string_values(json, state, headline_json_value);

    // Cleanup: free input arguments and allocated memory
    PG_FREE_IF_COPY(json, 1);
    PG_FREE_IF_COPY(query, 2);
    if (opt) PG_FREE_IF_COPY(opt, 3);
    pfree(prs.words);

    // Clean up headline markers if transformation occurred
    if (state->transformed) {
        pfree(prs.startsel);
        pfree(prs.stopsel);
    }

    PG_RETURN_TEXT_P(out);
}
```