# ts_headline_json_byid_opt

## Location
src/backend/tsearch/wparser.c: 443 - 490

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
  - `headline_json_value`: Action function for processing individual JSON string values
  - `transform_json_string_values`: Core JSON transformation function
  - `lookup_ts_config_cache`: Retrieves cached text search configuration
  - `lookup_ts_parser_cache`: Retrieves cached text search parser
  - `deserialize_deflist`: Parses headline options from text format
  - `palloc`: PostgreSQL memory allocation
  - `palloc0`: PostgreSQL zero-initialized memory allocation
  - `pfree`: PostgreSQL memory deallocation
  - Various PostgreSQL macros: `PG_GETARG_*`, `PG_FREE_IF_COPY`, `PG_RETURN_TEXT_P`
- Called from (representative examples):
  - `ts_headline_json`: Wrapper using current default configuration
  - `ts_headline_json_byid`: Wrapper without options parameter
  - `ts_headline_json_opt`: Wrapper using current default configuration with options

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c at lines 443-490
- This is the main implementation function for JSON headline generation functionality
- Includes error checking for parsers that do not support headline creation
- Uses a sophisticated JSON transformation approach that preserves JSON structure while highlighting text content
- Manages complex memory allocation patterns including dynamic word entry arrays
- The function supports optional headline generation parameters through the `opt` parameter
- Part of PostgreSQL full-text search functionality specifically designed for JSON document processing