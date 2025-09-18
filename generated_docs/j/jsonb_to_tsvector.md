# jsonb_to_tsvector

## Location
src/backend/tsearch/to_tsany.c: 344 - 363

## Overview
A PostgreSQL function that converts a JSONB value to a text search vector (TSVector) using the current default text search configuration and specified indexing flags.

## Definition
```c
Datum jsonb_to_tsvector(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper around the core JSONB to TSVector conversion functionality. It extracts two JSONB arguments from the PostgreSQL function call interface: the source JSONB data and a JSONB object containing indexing flags. The function retrieves the current default text search configuration, parses the indexing flags, and delegates the actual conversion work to `jsonb_to_tsvector_worker`. It handles proper memory management by freeing copied arguments and returns the resulting TSVector.

## Parameters / Member Variables
- `jb`: The source JSONB value to be converted to a text search vector
- `jbFlags`: A JSONB object containing flags that control how the JSONB data is processed during conversion

## Dependencies
- Functions called/Symbols referenced:
  - parse_jsonb_index_flags
  - getTSCurrentConfig
  - jsonb_to_tsvector_worker
  - PG_GETARG_JSONB_P
  - PG_FREE_IF_COPY
  - PG_RETURN_TSVECTOR
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Located in src/backend/tsearch/to_tsany.c:344-363
- This is a PostgreSQL C function that can be called from SQL
- Uses the current default text search configuration rather than taking it as a parameter
- Properly manages memory by freeing copied JSONB arguments
- Part of PostgreSQL's full-text search functionality for JSON/JSONB data types