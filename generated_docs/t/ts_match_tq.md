# ts_match_tq

## Location
src/backend/utils/adt/tsvector_op.c: 2266 - 2294

## Overview
A PostgreSQL function that performs text search matching between a text input and a TSQuery, converting the text to a TSVector internally before comparison.

## Definition

```c
Datum
ts_match_tq(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements text search matching with automatic text-to-tsvector conversion. It takes a text input (first argument) and a TSQuery (second argument), converts the text to a TSVector using the  function, and then performs the matching operation using . This function provides a convenient interface for text search operations where the input is raw text rather than a pre-processed TSVector.

The function follows PostgreSQL's function calling convention using  and returns a boolean result indicating whether the text matches the query criteria.

## Parameters / Member Variables
- : The input text to be converted to TSVector and matched
- : The TSQuery object containing the search criteria

## Dependencies
- Functions called/Symbols referenced:
  - : Converts text input to TSVector format
  - : Performs the actual matching between TSVector and TSQuery
  - : PostgreSQL function call mechanism for single-argument functions
  - : PostgreSQL function call mechanism for two-argument functions
  - : Converts Datum to TSVector
  - : Converts Datum to boolean
  - : Converts TSVector to Datum
  - : Converts TSQuery to Datum
  - : Macro to extract TSQuery argument
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function automatically handles memory management by freeing the temporary TSVector and TSQuery objects
- Uses PostgreSQL's memory management macros (, )
- This is a convenience function that combines text-to-tsvector conversion with matching in a single operation
- The function is part of PostgreSQL's full-text search functionality
- Located in 