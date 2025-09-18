# get_json_object_as_hash

## Location
src/backend/utils/adt/jsonfuncs.c: 3809 - 3850

## Overview
A static function that parses a JSON object string and decomposes it into a PostgreSQL hash table for efficient field access during record population operations.

## Definition


## Detailed Description
This function parses a JSON object and creates a hash table containing all the key-value pairs for efficient lookup during record population. It uses PostgreSQL's JSON parser with custom semantic actions to populate the hash table. The function sets up proper lexical context, configures semantic actions for JSON parsing events, and handles errors gracefully by cleaning up resources and returning NULL on parse failures.

Key behaviors:
- Creates a hash table with string keys (field names) and JsonHashEntry values
- Sets up JSON lexical context with proper encoding
- Configures semantic actions for JSON parsing events (arrays, scalars, object fields)
- Parses JSON using PostgreSQL's error-safe parser
- Cleans up resources and returns NULL on parse errors
- Uses current memory context for hash table allocation

## Parameters / Member Variables
- : Pointer to the JSON string to be parsed
- : Length of the JSON string in bytes
- : Name of the calling function (used for error reporting context)
- : Error context for soft error handling during parsing

## Dependencies
- Functions called/Symbols referenced:
  - hash_create
  - palloc0
  - makeJsonLexContextCstringLen
  - GetDatabaseEncoding
  - pg_parse_json_or_errsave
  - hash_destroy
  - freeJsonLexContext
  - hash_array_start
  - hash_scalar
  - hash_object_field_start
  - hash_object_field_end
  - HASH_ELEM
  - HASH_STRINGS
  - HASH_CONTEXT
- Called from (representative examples):
  - JsValueToJsObject

## Notes and Other Information
- This is a static function used internally by JSON processing infrastructure
- Creates hash table with NAMEDATALEN key size to accommodate PostgreSQL identifier limits
- Uses semantic actions to handle different JSON elements during parsing
- Implements proper resource cleanup on parsing failures
- Part of the JSON object access optimization for record population
- The hash table enables O(1) field lookup instead of linear JSON traversal
- Uses PostgreSQL's standard hash table implementation with string keys
- Handles encoding properly through GetDatabaseEncoding()