# JHashState

## Location
src/backend/utils/adt/jsonfuncs.c: 135 - 143

## Overview
JHashState is a structure that maintains state information for the get_json_object_as_hash functionality, which converts JSON objects into PostgreSQL hash table structures.

## Definition


## Detailed Description
JHashState serves as a context structure for PostgreSQL's JSON-to-hash conversion functionality. It encapsulates the necessary state information required to parse JSON objects and convert them into PostgreSQL's internal hash table format (HTAB). The structure coordinates JSON lexical parsing, hash table management, and token state preservation during the conversion process.

## Parameters / Member Variables
- : Pointer to JsonLexContext for JSON lexical analysis and parsing
- : Name of the function being executed (for error reporting and debugging)
- hash: hash table empty: Pointer to PostgreSQL hash table (HTAB) where JSON key-value pairs are stored
- : Temporarily saved scalar value during parsing
- : Pointer to the start position of saved JSON content
- : JsonTokenType representing the type of the last saved token

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](JsonLexContext.md)
  - [HTAB](../H/HTAB.md)
  - [JsonTokenType](JsonTokenType.md)
- Called from (representative examples):
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - [hash_object_field_start](../h/hash_object_field_start.md)
  - [hash_object_field_end](../h/hash_object_field_end.md)
  - [hash_array_start](../h/hash_array_start.md)
  - [hash_scalar](../h/hash_scalar.md)

## Notes and Other Information
This structure is specifically designed for converting JSON objects into PostgreSQL's internal hash table format, which provides efficient key-value lookup capabilities. The saved_scalar and save_json_start fields are used to temporarily store parsing state when the parser needs to look ahead or preserve tokens for later processing. The hash table format allows for efficient access to JSON object properties within PostgreSQL's internal operations.