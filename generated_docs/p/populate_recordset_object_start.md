# populate_recordset_object_start

## Location
src/backend/utils/adt/jsonfuncs.c: 4213 - 4242

## Overview
A static function that handles the start of a JSON object during JSON recordset population, setting up a hash table for object key-value pairs at the appropriate nesting level.

## Definition


## Detailed Description
This function is a callback handler for JSON parsing that is invoked when a JSON object opening brace is encountered. It manages the creation of hash tables for JSON objects that represent individual records in a recordset. The function enforces that the top-level JSON structure must be an array (not an object) and creates a new hash table for objects at nesting level 1, which represent individual records.

The function implements different behaviors based on the lexical nesting level:
- Level 0: Rejects objects at the top level, requiring an array instead
- Level 1: Creates a new hash table to store key-value pairs for the current record
- Level > 1: No special processing for nested objects within records

## Parameters / Member Variables
- : A void pointer that is cast to PopulateRecordsetState, containing the parsing state including the lexer and function name for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - PopulateRecordsetState (struct type)
  - HASHCTL (struct type)
  - JSON_SUCCESS (return value constant)
  - NAMEDATALEN (constant for hash key size)
  - JsonHashEntry (struct type for hash table entries)
  - hash_create (function to create hash table)
  - HASH_ELEM, HASH_STRINGS, HASH_CONTEXT (hash table creation flags)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - populate_recordset_worker
  - JsObjectFree

## Notes and Other Information
- This function is part of the JSON recordset population infrastructure in PostgreSQL
- The hash table created uses NAMEDATALEN as the key size, which is the standard size for PostgreSQL identifiers
- Error reporting uses the function name stored in the state for context-appropriate error messages
- The hash table is created in the current memory context and will be automatically cleaned up when the context is destroyed