# populate_recordset_object_end

## Location
[src/backend/utils/adt/jsonfuncs.c:4243-4265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4243-L4265)

## Overview
A static function that handles the end of a JSON object during JSON recordset population, processing completed records and cleaning up associated hash tables.

## Definition
```c
static JsonParseErrorType populate_recordset_object_end(void *state)
```

## Detailed Description
This function is a callback handler for JSON parsing that is invoked when a JSON object closing brace is encountered. It completes the processing of JSON objects that represent individual records in a recordset. When a level-1 object (representing a complete record) is finished, the function creates a JsObject structure from the accumulated hash table data and calls populate_recordset_record to convert it into a tuple record.

The function implements different behaviors based on the lexical nesting level:
- Level > 1: No special processing for nested objects within records
- Level 1: Process the completed record by calling populate_recordset_record and clean up the hash table

After processing a record, the function destroys the hash table to free memory and sets the hash pointer to NULL.

## Parameters / Member Variables
- `state`: A void pointer that is cast to PopulateRecordsetState, containing the parsing state including the lexer and the current JSON hash table

## Dependencies
- Functions called/Symbols referenced:
  - [PopulateRecordsetState](../P/PopulateRecordsetState.md) (struct type)
  - [JsObject](../J/JsObject.md) (struct type for JSON object representation)
  - JSON_SUCCESS (return value constant)
  - [populate_recordset_record](populate_recordset_record.md) (function to process a complete record)
  - [hash_destroy](../h/hash_destroy.md) (function to clean up hash table)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [populate_recordset_worker](populate_recordset_worker.md)
  - JsObjectFree

## Notes and Other Information
- This function is part of the JSON recordset population infrastructure in PostgreSQL
- The JsObject created has its is_json flag set to true to indicate it contains JSON hash data
- Memory cleanup is crucial - the hash table is destroyed after processing each record
- Works in tandem with populate_recordset_object_start to bracket the processing of individual records
- The function relies on populate_recordset_record (already documented) to convert the hash table data into PostgreSQL tuple format

## Simplified Source

```c
static JsonParseErrorType populate_recordset_object_end(void *state) {
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
    JsObject obj;

    // Nested objects need no special processing
    if (_state->lex->lex_level > 1)
        return JSON_SUCCESS;

    // Create JSON object from accumulated hash table
    obj.is_json = true;
    obj.val.json_hash = _state->json_hash;

    // Process completed record from level-1 object
    populate_recordset_record(_state, &obj);

    // Clean up hash table after processing
    hash_destroy(_state->json_hash);
    _state->json_hash = NULL;

    return JSON_SUCCESS;
}
```