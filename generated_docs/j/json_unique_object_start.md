# json_unique_object_start

## Location
src/backend/utils/adt/json.c: 1606 - 1623

## Overview
Semantic action function for JSON parsing that initializes object tracking when enforcing key uniqueness constraints.

## Definition


## Detailed Description
The `json_unique_object_start` function is a semantic action callback used during JSON parsing to handle the start of JSON objects when key uniqueness validation is enabled. It creates and pushes a new stack entry to track the current object's unique identifier and maintains the parsing state stack hierarchy. This function is part of PostgreSQL's JSON validation infrastructure that ensures objects don't contain duplicate keys.

## Parameters / Member Variables
- `_state`: Void pointer to JsonUniqueParsingState structure containing parsing context and uniqueness tracking information

## Dependencies
- Functions called/Symbols referenced:
  - JsonUniqueParsingState (type cast)
  - JsonUniqueStackEntry (type)
  - [palloc](../p/palloc.md)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [json_validate](json_validate.md)

## Notes and Other Information
- Returns JSON_SUCCESS immediately if uniqueness checking is disabled
- Allocates memory for new stack entry using palloc
- Assigns unique object identifier using incremental counter
- Maintains parent-child stack relationship for nested objects
- Part of JSON parsing semantic actions framework
- Static function scope limits visibility to json.c compilation unit
- Essential for implementing JSON object key uniqueness validation
- Works in conjunction with json_unique_object_end to manage object lifecycle
- Stack-based approach handles arbitrary nesting levels efficiently