# ts_headline_jsonb

## Location
src/backend/tsearch/wparser.c: 415 - 423

## Overview
A PostgreSQL function that generates highlighted headlines from JSONB documents based on a text search query, using the current default text search configuration.

## Definition


## Detailed Description
 is a convenience wrapper function that provides a simplified interface for generating text search headlines from JSONB documents. It automatically uses the current default text search configuration (obtained via ) and delegates the actual processing to . This function is ideal when you want to highlight search terms in JSONB documents without needing to specify a particular text search configuration or custom options.

The function recursively processes all string values within the JSONB document, applying headline generation to each string based on the provided search query.

## Parameters / Member Variables
- First parameter (): The input JSONB document to be processed for headline generation
- Second parameter (): The text search query used to identify terms for highlighting

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_jsonb_byid_opt](ts_headline_jsonb_byid_opt.md): The core function that performs the actual JSONB headline generation
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md): Retrieves the current default text search configuration
  - DirectFunctionCall3: PostgreSQL macro for calling functions with 3 arguments
  - PG_RETURN_DATUM: PostgreSQL macro for returning function results
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a convenience wrapper that uses default settings for text search configuration
- The actual JSONB headline generation logic is implemented in 
- Located in src/backend/tsearch/wparser.c:415-423
- Part of PostgreSQL's full-text search functionality for JSON/JSONB data types
- Uses the system's default text search configuration rather than requiring explicit specification
- Processes all string values within the JSONB document recursively
- Similar to  but specifically designed for JSONB document types