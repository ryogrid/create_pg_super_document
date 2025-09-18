# ts_headline_jsonb_byid

## Location
src/backend/tsearch/wparser.c: 424 - 432

## Overview
A PostgreSQL function that generates highlighted headlines from JSONB documents based on a text search query, using a specified text search configuration.

## Definition


## Detailed Description
 is a wrapper function that provides JSONB headline generation with a specific text search configuration but without custom options. It allows users to specify which text search configuration to use (by providing the configuration's OID) rather than using the default system configuration. The function delegates the actual processing to  while providing a simpler interface that doesn't require specifying custom highlighting options.

This function is useful when you need to use a specific text search configuration (for example, for different languages or custom parsing rules) but don't need to customize the highlighting behavior beyond the defaults.

## Parameters / Member Variables
- First parameter (): The OID of the text search configuration to use
- Second parameter (): The input JSONB document to be processed for headline generation
- Third parameter (): The text search query used to identify terms for highlighting

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_jsonb_byid_opt](ts_headline_jsonb_byid_opt.md): The core function that performs the actual JSONB headline generation with options
  - DirectFunctionCall3: PostgreSQL macro for calling functions with 3 arguments
  - PG_RETURN_DATUM: PostgreSQL macro for returning function results
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function provides a middle ground between  (default config, no options) and  (custom config with options)
- The actual JSONB headline generation logic is implemented in 
- Located in src/backend/tsearch/wparser.c:424-432
- Part of PostgreSQL's full-text search functionality for JSON/JSONB data types
- Allows specification of text search configuration but uses default highlighting options
- Processes all string values within the JSONB document recursively
- Uses DirectFunctionCall3 because it doesn't pass custom options (NULL is passed for the options parameter)