# parse_jsonb_index_flags

## Location
src/backend/utils/adt/jsonfuncs.c: 5572 - 5639

## Overview
parse_jsonb_index_flags parses a JSONB array containing string flags that specify which JSON value types should be included when iterating over JSON documents in text search operations.

## Definition
```c
uint32
parse_jsonb_index_flags(Jsonb *jb)
```

## Detailed Description
This function takes a JSONB value containing an array of string flags and converts them into a bitmask that controls which types of JSON values should be processed during iteration. It's primarily used by text search functions (jsonb_to_tsvector, json_to_tsvector) to specify which JSON value types should be indexed.

The function accepts the following flag strings (case-insensitive):
- **"all"**: Include all value types (equivalent to setting all other flags)
- **"key"**: Include object keys  
- **"string"**: Include string values
- **"numeric"**: Include numeric values
- **"boolean"**: Include boolean values

The function iterates through the input array, validates each element as a string, matches it against known flag names, and builds a bitmask using the corresponding jti* constants. It includes comprehensive error checking for invalid input formats and unknown flag names.

## Parameters / Member Variables
- `jb`: Pointer to the input Jsonb structure containing the array of flag strings

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pg_strncasecmp](pg_strncasecmp.md)
  - [pnstrdup](pnstrdup.md)
  - WJB_BEGIN_ARRAY, WJB_ELEM, WJB_END_ARRAY, WJB_DONE
  - jbvString
  - jtiAll, jtiKey, jtiString, jtiNumeric, jtiBool (flag constants)
- Called from (representative examples):
  - [jsonb_to_tsvector_byid](../j/jsonb_to_tsvector_byid.md)
  - [jsonb_to_tsvector](../j/jsonb_to_tsvector.md)
  - [json_to_tsvector_byid](../j/json_to_tsvector_byid.md)  
  - [json_to_tsvector](../j/json_to_tsvector.md)

## Notes and Other Information
- The function accepts both arrays and scalars since scalars are internally represented as single-element arrays
- Uses case-insensitive string comparison for flag matching via pg_strncasecmp
- Returns a uint32 bitmask combining the selected jti* flag constants
- Provides detailed error messages with hints about valid flag values
- The jti* constants correspond to JSON Type Iterator flags used in text search indexing
- This function is part of the text search integration for JSON/JSONB data types
- Flag names are chosen to match the output of jsonb_typeof function for consistency