# JsonHashEntry

## Location
src/backend/utils/adt/jsonfuncs.c: 146 - 151

## Overview
JsonHashEntry is a structure that represents an individual entry in a hash table used for storing JSON object field names and their corresponding values.

## Definition


## Detailed Description
JsonHashEntry serves as the fundamental data structure for storing JSON object fields within PostgreSQL's hash table implementation. Each entry represents a single key-value pair from a JSON object, where the field name serves as the hash key and the associated value and type information are stored alongside it. The structure is designed to be compatible with PostgreSQL's hash table system (HTAB) requirements.

## Parameters / Member Variables
- : Fixed-size character array storing the JSON field name as the hash key (must be the first field for hash table compatibility)
- : Pointer to the string representation of the JSON field's value
- : JsonTokenType indicating the type of the JSON value (string, number, boolean, null, object, array)

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN
  - JsonTokenType
- Called from (representative examples):
  - JsObjectGetField
  - get_json_object_as_hash
  - hash_object_field_end
  - populate_recordset_object_start
  - populate_recordset_object_field_end

## Notes and Other Information
The fname field is specifically marked as "MUST BE FIRST" because PostgreSQL's hash table implementation requires the hash key to be at the beginning of the structure. The NAMEDATALEN constant defines the maximum length for PostgreSQL identifiers, ensuring compatibility with PostgreSQL's naming conventions. This structure is essential for JSON-to-relational mapping operations and efficient field lookup within JSON objects.