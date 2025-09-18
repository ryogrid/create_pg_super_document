# ts_headline_opt

## Location
[src/backend/tsearch/wparser.c:357-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L357-L366)

## Overview
A PostgreSQL function that generates highlighted headlines from text based on a text search query with custom options, using the current default text search configuration.

## Definition


## Detailed Description
 is a wrapper function that extends the basic headline functionality by allowing custom highlighting options. Like , it automatically uses the current default text search configuration but accepts an additional parameter for customizing the headline generation behavior. The function delegates the actual processing to  while providing a simpler interface that doesn't require specifying a text search configuration ID.

This function is useful when you need more control over the headline generation process (such as custom start/stop tags, fragment length, etc.) but still want to use the default text search configuration.

## Parameters / Member Variables
- First parameter (): The input text to be processed for headline generation
- Second parameter (): The text search query used to identify terms for highlighting
- Third parameter (): Options text specifying custom highlighting parameters (e.g., StartSel, StopSel, MaxWords, MinWords, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_byid_opt](ts_headline_byid_opt.md): The core function that performs the actual headline generation with options
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md): Retrieves the current default text search configuration
  - DirectFunctionCall4: PostgreSQL macro for calling functions with 4 arguments
  - PG_RETURN_DATUM: PostgreSQL macro for returning function results
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is an enhanced version of  that accepts custom options
- The actual headline generation logic is implemented in 
- Located in src/backend/tsearch/wparser.c:357-366
- Part of PostgreSQL's full-text search functionality
- Uses DirectFunctionCall4 instead of DirectFunctionCall3 due to the additional options parameter
- The options parameter allows fine-tuning of highlighting behavior such as custom delimiters and fragment length