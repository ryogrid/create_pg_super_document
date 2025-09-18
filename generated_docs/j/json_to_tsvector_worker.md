# json_to_tsvector_worker

## Location
src/backend/tsearch/to_tsany.c: 364 - 379

## Overview
A static worker function that converts JSON text data to a text search vector (TSVector) using the specified text search configuration and indexing flags.

## Definition
```c
static TSVector json_to_tsvector_worker(Oid cfgId, text *json, uint32 flags)
```

## Detailed Description
This function serves as the core worker for converting JSON text data to TSVector format. It initializes a TSVectorBuildState structure and a ParsedText structure to manage the conversion process. The function uses `iterate_json_values` to traverse through the JSON data, applying the specified flags to control which parts of the JSON are processed. For each relevant value found, it calls `add_to_tsvector` to build up the text search vector. Finally, it constructs and returns the completed TSVector using `make_tsvector`.

## Parameters / Member Variables
- `cfgId`: The OID of the text search configuration to use for processing
- `json`: A pointer to the JSON text data to be converted
- `flags`: Bit flags controlling which parts of the JSON should be indexed (keys, values, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - iterate_json_values
  - add_to_tsvector
  - make_tsvector
  - TSVectorBuildState (struct)
  - ParsedText (struct)
- Called from (representative examples):
  - json_string_to_tsvector_byid
  - json_string_to_tsvector
  - json_to_tsvector_byid
  - json_to_tsvector

## Notes and Other Information
- Located in src/backend/tsearch/to_tsany.c:364-379
- Static function, only accessible within the same source file
- Shared worker function for multiple JSON to TSVector conversion functions
- Initializes ParsedText structure with NULL words and 0 curwords
- Uses callback-based iteration through JSON values for flexible processing
- Part of PostgreSQL's full-text search functionality for JSON data types