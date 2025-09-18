# jsonb_to_tsvector_worker

## Location
src/backend/tsearch/to_tsany.c: 285 - 300

## Overview
Internal worker function that converts JSONB data to a text search vector (TSVector) using the specified text search configuration and flags.

## Definition


## Detailed Description
The  function is a core internal worker function that handles the conversion of JSONB (binary JSON) data structures into text search vectors. It serves as the common implementation used by various public JSONB-to-TSVector conversion functions. The function iterates through JSONB values based on the provided flags, extracting text content and building a TSVector using the specified text search configuration.

The function initializes a TSVectorBuildState and ParsedText structure to manage the conversion process, then delegates to  to traverse the JSONB structure and collect text content via the  callback function.

## Parameters / Member Variables
- : Object ID of the text search configuration to use for parsing and processing
- : Pointer to the JSONB structure to be converted
- : Bit flags controlling which JSONB values to include (e.g., keys, values, strings, numerics)

## Dependencies
- Functions called/Symbols referenced:
  - : Traverses JSONB structure and calls callback for matching values
  - : Callback function that processes individual text values
  - : Constructs the final TSVector from parsed text data
  - : State structure for building TSVectors
  - : Structure containing parsed words and metadata
  - : JSONB data type
- Called from (representative examples):
  - : Converts JSONB string values to TSVector with specific config
  - : Converts JSONB string values to TSVector with default config
  - : Converts JSONB to TSVector with specific config
  - : Converts JSONB to TSVector with default config

## Notes and Other Information
- This is a static (internal) function, not directly callable from SQL
- Provides the core implementation shared by all JSONB-to-TSVector conversion functions
- The flags parameter allows selective processing of JSONB components (keys vs values, etc.)
- Located in 
- Part of PostgreSQL's full-text search system for JSON data