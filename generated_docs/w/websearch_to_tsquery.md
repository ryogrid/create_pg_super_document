# websearch_to_tsquery

## Location
src/backend/tsearch/to_tsany.c: 718 - 727

## Overview
Converts a web search query string to a tsquery using the current default text search configuration.

## Definition
```c
Datum websearch_to_tsquery(PG_FUNCTION_ARGS)
```

## Detailed Description
The `websearch_to_tsquery` function is a PostgreSQL built-in function that converts a web-style search query into a `tsquery` object for full-text searching. This function acts as a wrapper that automatically uses the current default text search configuration (obtained via `default_text_search_config` parameter) to parse the input query.

The function internally delegates to `websearch_to_tsquery_byid`, which performs the actual parsing using web search syntax rules. The web search syntax allows for more intuitive query input compared to the standard tsquery format, making it easier for end users to construct text search queries.

The parsing is done with `P_TSQ_WEB` flag, which enables web-style query parsing that handles quoted phrases, implicit AND operations between terms, and other user-friendly search patterns.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `in` (text): The web search query string to be converted

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`: Extracts text argument from function args
  - `getTSCurrentConfig`: Retrieves the current default text search configuration OID
  - `DirectFunctionCall2`: Calls another PostgreSQL function directly
  - `websearch_to_tsquery_byid`: Performs the actual query parsing with configuration ID
  - `ObjectIdGetDatum`: Converts OID to Datum
  - `PointerGetDatum`: Converts pointer to Datum
  - `PG_RETURN_DATUM`: Returns the result as a Datum

- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function is typically exposed as the SQL function `websearch_to_tsquery(text)` 
- The function automatically uses the current text search configuration, making it convenient for applications that dont need to specify a particular configuration
- The underlying `websearch_to_tsquery_byid` uses `OP_PHRASE` as the default query operator, ensuring that word positions in complex morphemes match exactly with the tsvector
- Web search syntax is more user-friendly than the standard tsquery syntax, allowing natural language-like queries
- Located in `src/backend/tsearch/to_tsany.c` at lines 718-727