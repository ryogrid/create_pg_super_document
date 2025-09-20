# populate_recordset_object_field_end

## Location
[src/backend/utils/adt/jsonfuncs.c:4328-4387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4328-L4387)

## Overview
This function handles the completion of JSON object field processing during JSON-to-recordset conversion, storing field values in a hash table for later tuple construction.

## Definition

```c
static JsonParseErrorType
populate_recordset_object_field_end(void *state, char *fname, bool isnull)
```
## Detailed Description
The `populate_recordset_object_field_end` function is a callback handler that processes the end of object fields during JSON parsing for recordset conversion. It serves as the counterpart to `populate_recordset_object_field_start` and is responsible for actually storing the parsed field data.

The function performs several critical operations:
1. Validates field names against PostgreSQL's NAMEDATALEN limit to prevent hash collisions
2. Creates or updates hash entries for each JSON field, allowing duplicate field names (later values override earlier ones)
3. Stores the appropriate field value based on the token type saved during field start processing
4. Handles both scalar values and complex JSON structures (arrays/objects) by storing either the saved scalar or the complete JSON substring

The function uses a hash table (`_state->json_hash`) to efficiently map field names to their values and types, which is later used for tuple construction in the recordset conversion process.

## Parameters / Member Variables
- `state`: A void pointer to `PopulateRecordsetState` structure containing parsing context and hash table for field storage
- `fname`: Character pointer to the field name being processed (must be less than NAMEDATALEN)  
- `isnull`: Boolean flag indicating whether the field value is null, must match saved token type

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (hash table operations)
  - [palloc](palloc.md) (memory allocation)
  - memcpy (memory copying)
  - strlen (string length calculation)
  - Assert (debugging assertion)
  - HASH_ENTER (hash operation constant)
  - NAMEDATALEN (PostgreSQL name length limit)
  - JSON_SUCCESS (return constant)
  - JSON_TOKEN_NULL (token type constant)
  - [JsonHashEntry](../J/JsonHashEntry.md) (hash table entry structure)
  - [PopulateRecordsetState](../P/PopulateRecordsetState.md) (state structure)

- Called from (representative examples):
  - [populate_recordset_worker](populate_recordset_worker.md)
  - JsObjectFree

## Notes and Other Information
- This is a static function accessible only within jsonfuncs.c
- Field names equal to or longer than NAMEDATALEN are ignored to prevent hash collisions with truncated PostgreSQL column names
- Duplicate field names are handled by overriding previous values rather than generating errors
- The function stores either scalar values or complete JSON substrings depending on the token type
- Memory for JSON substring values is allocated using PostgreSQL's memory context system (palloc)
- The hash table created here is later used by tuple construction functions to build the final recordset
- This function maintains the exact equality requirement for field names rather than allowing truncation-based matching