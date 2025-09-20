# json_unique_check_key

## Location
[src/backend/utils/adt/json.c:949-968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L949-L968)

## Overview
This function checks whether a given key is unique within a JSON object being processed, maintaining uniqueness state for duplicate key detection.

## Definition

```c
static bool
json_unique_check_key(JsonUniqueCheckState *cxt, const char *key, int object_id)
```
## Detailed Description
The function implements duplicate key detection for JSON objects by maintaining a hash table of seen keys. It takes a key string and an object identifier, creates a hash entry for the key, and attempts to insert it into the uniqueness checking hash table. The function returns true if the key is unique (not previously seen) and false if it's a duplicate.

The function uses PostgreSQL's hash table infrastructure to efficiently track keys that have already been encountered during JSON object processing.

## Parameters / Member Variables
- : Pointer to JsonUniqueCheckState containing the hash table for tracking unique keys
- : The key string to check for uniqueness
- : Integer identifier for the JSON object being processed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - strlen
- Data structures used:
  - JsonUniqueCheckState
  - JsonUniqueHashEntry
  - HASH_ENTER
- Called from (representative examples):
  - [json_object_agg_transfn_worker](json_object_agg_transfn_worker.md)
  - [json_build_object_worker](json_build_object_worker.md)
  - [json_unique_object_field_start](json_unique_object_field_start.md)

## Notes and Other Information
- This is a static function, only accessible within the json.c compilation unit
- The function modifies the hash table state by inserting new entries
- Returns boolean value indicating uniqueness (true = unique, false = duplicate)
- Part of PostgreSQL's JSON processing infrastructure for ensuring object key uniqueness