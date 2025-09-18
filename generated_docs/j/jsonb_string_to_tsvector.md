# jsonb_string_to_tsvector

## Location
src/backend/tsearch/to_tsany.c: 314 - 327

## Overview
Converts string values from JSONB data to a text search vector (TSVector) using the current default text search configuration.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that extracts string values from JSONB data and converts them into a text search vector using the current default text search configuration. This function is a convenience wrapper that automatically uses the session's default text search configuration, eliminating the need to explicitly specify a configuration ID.

Like its  counterpart, this function specifically targets only string values within the JSONB structure, filtering out other data types such as numbers, booleans, nulls, or nested objects/arrays. The function retrieves the current default configuration and then delegates to the core worker function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : JSONB data structure to process (retrieved via )

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the current default text search configuration
  - : Core worker function that performs the JSONB-to-TSVector conversion
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
- Automatically uses the current session's default text search configuration
- For explicit configuration control, use  instead
- Located in 
- Memory management includes proper cleanup of JSONB input parameter
- Provides a simpler interface compared to the  variant by eliminating configuration parameter