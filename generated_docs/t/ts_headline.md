# ts_headline

## Location
src/backend/tsearch/wparser.c: 348 - 356

## Overview
A PostgreSQL function that generates highlighted headlines from text based on a text search query, using the current default text search configuration.

## Definition


## Detailed Description
 is a wrapper function that provides a simplified interface for generating text search headlines. It automatically uses the current default text search configuration (obtained via ) and delegates the actual work to . This function is typically used when you want to generate highlighted text snippets without specifying a particular text search configuration or custom options.

The function takes two arguments (accessed via ) and returns a highlighted version of the input text with search terms emphasized according to the default highlighting rules.

## Parameters / Member Variables
- First parameter (): The input text to be processed for headline generation
- Second parameter (): The text search query used to identify terms for highlighting

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_byid_opt](ts_headline_byid_opt.md): The core function that performs the actual headline generation
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md): Retrieves the current default text search configuration
  - DirectFunctionCall3: PostgreSQL macro for calling functions with 3 arguments
  - PG_RETURN_DATUM: PostgreSQL macro for returning function results
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a convenience wrapper that uses default settings for text search configuration
- The actual headline generation logic is implemented in 
- Located in src/backend/tsearch/wparser.c:348-356
- Part of PostgreSQL's full-text search functionality
- Uses the system's default text search configuration rather than requiring explicit specification