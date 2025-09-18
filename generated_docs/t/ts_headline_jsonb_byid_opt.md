# ts_headline_jsonb_byid_opt

## Location
src/backend/tsearch/wparser.c: 367 - 414

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
  - lookup_ts_config_cache: Retrieves cached text search configuration
  - lookup_ts_parser_cache: Retrieves cached text search parser
  - deserialize_deflist: Parses options text into a list structure
  - transform_jsonb_string_values: Core function that recursively processes JSONB string values
  - headline_json_value: Callback function for applying headlines to individual string values
  - palloc/palloc0: PostgreSQL memory allocation functions
  - pfree: PostgreSQL memory deallocation function
- Called from (representative examples):
  - ts_headline_jsonb: Wrapper using default configuration
  - ts_headline_jsonb_byid: Wrapper without custom options
  - ts_headline_jsonb_opt: Wrapper with custom options using default configuration

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c:367-414
- This is the most comprehensive JSONB headline function, supporting all customization options
- Uses HeadlineJsonState structure to maintain state during JSONB traversal
- Performs error checking to ensure the text search parser supports headline creation
- Handles memory management with proper cleanup of allocated structures
- The function processes all string values in the JSONB document recursively
- Uses PG_FREE_IF_COPY macros for proper memory management of PostgreSQL function arguments
- Part of PostgreSQL's full-text search functionality for JSON/JSONB data types