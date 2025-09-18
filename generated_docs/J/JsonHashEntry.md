# JsonHashEntry

## Location
[src/backend/utils/adt/jsonfuncs.c:146-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L146-L151)

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
  - [JsonTokenType](JsonTokenType.md)
- Called from (representative examples):
  - [JsObjectGetField](JsObjectGetField.md)
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - [hash_object_field_end](../h/hash_object_field_end.md)
  - [populate_recordset_object_start](../p/populate_recordset_object_start.md)
  - [populate_recordset_object_field_end](../p/populate_recordset_object_field_end.md)

## Notes and Other Information
The fname field is specifically marked as "MUST BE FIRST" because PostgreSQL's hash table implementation requires the hash key to be at the beginning of the structure. The NAMEDATALEN constant defines the maximum length for PostgreSQL identifiers, ensuring compatibility with PostgreSQL's naming conventions. This structure is essential for JSON-to-relational mapping operations and efficient field lookup within JSON objects.