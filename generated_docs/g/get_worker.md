# get_worker

## Location
src/backend/utils/adt/jsonfuncs.c: 1101 - 1157

## Overview
A common worker function that extracts specific fields or array elements from JSON objects by parsing the JSON text and navigating through specified paths.

## Definition


## Detailed Description
The  function serves as the core implementation for all JSON getter functions in PostgreSQL. It parses a JSON object in text form and extracts values at specified paths, which can include both object field names and array indices. The function sets up a JSON lexical context and semantic actions, then uses the JSON parser to traverse the structure according to the provided path specifications. It supports both object field extraction via text paths and array element extraction via integer indices, with optional result normalization.

## Parameters / Member Variables
- : The JSON object in text form to be parsed
- : Array of field name strings to extract from nested objects (can be NULL or contain NULL entries)
- : Array of zero-based integer indices to extract from nested arrays (can be NULL or contain INT_MIN entries, supports negative indices)
- : The length of both tpath[] and ipath[] arrays
- : Boolean flag indicating whether to de-escape string and null scalars in the results

## Dependencies
- Functions called/Symbols referenced:
  - JsonSemAction
  - GetState
  - makeJsonLexContext
  - get_scalar
  - get_object_start
  - get_object_end
  - get_array_start
  - get_array_end
  - get_object_field_start
  - get_object_field_end
  - get_array_element_start
  - get_array_element_end
  - pg_parse_json_or_ereport
  - freeJsonLexContext
- Called from (representative examples):
  - json_object_field
  - json_object_field_text
  - json_array_element
  - json_array_element_text
  - get_path_all

## Notes and Other Information
- The function is static and internal to jsonfuncs.c
- It efficiently sets only the semantic routines that are actually needed based on the path parameters
- Path matching is flexible: NULL entries in tpath skip object field matching at that level, and INT_MIN entries in ipath skip array element matching
- The function handles both simple field/element extraction and complex nested path navigation
- Memory management includes proper cleanup of the JSON lexical context
- Negative array indices are supported for accessing elements from the end of arrays