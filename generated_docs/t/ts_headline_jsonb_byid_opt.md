# ts_headline_jsonb_byid_opt

## Location
[src/backend/tsearch/wparser.c:367-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L367-L414)

## Overview
A PostgreSQL function that generates highlighted headlines from JSONB documents based on a text search query, with support for custom text search configurations and highlighting options.

## Definition


## Detailed Description
 is the core function for applying text search highlighting to JSONB documents. It recursively processes JSONB values, identifying string values and applying headline generation to them based on the provided TSQuery. The function supports custom text search configurations and highlighting options, making it the most flexible of the JSONB headline functions.

The function sets up a parsing context with the specified configuration, initializes headline generation structures, and uses  to recursively apply highlighting to all string values within the JSONB document. It handles memory management carefully, including proper cleanup of allocated structures.

## Parameters / Member Variables
-  (): Object ID of the text search configuration to use
-  (): The input JSONB document to process for headline generation
-  (): The text search query used to identify terms for highlighting
-  (, optional): Options text specifying custom highlighting parameters

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_ts_config_cache](../l/lookup_ts_config_cache.md): Retrieves cached text search configuration
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md): Retrieves cached text search parser
  - deserialize_deflist: Parses options text into a list structure
  - [transform_jsonb_string_values](transform_jsonb_string_values.md): Core function that recursively processes JSONB string values
  - [headline_json_value](../h/headline_json_value.md): Callback function for applying headlines to individual string values
  - [palloc](../p/palloc.md)/palloc0: PostgreSQL memory allocation functions
  - [pfree](../p/pfree.md): PostgreSQL memory deallocation function
- Called from (representative examples):
  - [ts_headline_jsonb](ts_headline_jsonb.md): Wrapper using default configuration
  - [ts_headline_jsonb_byid](ts_headline_jsonb_byid.md): Wrapper without custom options
  - [ts_headline_jsonb_opt](ts_headline_jsonb_opt.md): Wrapper with custom options using default configuration

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c:367-414
- This is the most comprehensive JSONB headline function, supporting all customization options
- Uses HeadlineJsonState structure to maintain state during JSONB traversal
- Performs error checking to ensure the text search parser supports headline creation
- Handles memory management with proper cleanup of allocated structures
- The function processes all string values in the JSONB document recursively
- Uses PG_FREE_IF_COPY macros for proper memory management of PostgreSQL function arguments
- Part of PostgreSQL's full-text search functionality for JSON/JSONB data types