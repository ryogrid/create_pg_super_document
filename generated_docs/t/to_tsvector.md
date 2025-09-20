# to_tsvector

## Location
[src/backend/tsearch/to_tsany.c:270-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L270-L284)

## Overview
Converts text input to a text search vector (TSVector) using the current default text search configuration.

## Definition

```c
Datum
to_tsvector(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that converts text input into a text search vector (TSVector). It serves as a convenience wrapper that automatically uses the current default text search configuration to process the input text. The function retrieves the current text search configuration ID and then delegates the actual processing to  with that configuration.

This is one of the primary entry points for full-text search functionality in PostgreSQL, allowing users to create searchable text vectors without explicitly specifying a text search configuration.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Text input to be converted to TSVector (retrieved via )

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the current default text search configuration
  - : Performs the actual text-to-TSVector conversion with a specific configuration ID
  - : PostgreSQL function call mechanism
  - : Macro for returning function result
  - : The result data type
- Called from (representative examples):
  - : Text search matching function
  - : Text search query matching function

## Notes and Other Information
- This function is part of PostgreSQL's full-text search system
- It automatically uses the current session's default text search configuration
- For explicit configuration control, use  instead
- The function is located in 