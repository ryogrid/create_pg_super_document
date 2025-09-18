# jsonb_string_to_tsvector_byid

## Location
src/backend/tsearch/to_tsany.c: 301 - 313

## Overview
Converts string values from JSONB data to a text search vector (TSVector) using a specified text search configuration.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that extracts string values from JSONB data and converts them into a text search vector using a specific text search configuration. Unlike functions that process all JSONB content, this function specifically targets only string values within the JSONB structure, filtering out other data types like numbers, booleans, or nested objects/arrays.

The function takes a configuration ID and JSONB input, then delegates to  with the  flag to indicate that only string values should be processed during JSONB traversal.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Object ID of the text search configuration (retrieved via )  
  - : JSONB data structure to process (retrieved via )

## Dependencies
- Functions called/Symbols referenced:
  - : Core worker function that performs the JSONB-to-TSVector conversion
  - : Macro to extract OID argument
  - : Macro to extract JSONB argument
  - : Memory management for JSONB input
  - : Macro for returning TSVector result
  - : Flag constant indicating string-only processing
  - : Result data type
  - : Input data type
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's full-text search system for JSON data
- Only processes string values from JSONB, ignoring numbers, booleans, nulls, and nested structures
- Requires explicit specification of text search configuration ID
- For default configuration behavior, use  instead
- Located in 
- Memory management includes proper cleanup of JSONB input parameter