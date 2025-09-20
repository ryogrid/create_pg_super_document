# JsonbContainerTypeName

## Location
[src/backend/utils/adt/jsonb.c:159-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L159-L179)

## Overview
A static utility function that determines and returns the type name of a JSONB container as a string representation.

## Definition

```c
static const char *
JsonbContainerTypeName(JsonbContainer *jbc)
```
## Detailed Description
This function analyzes a JSONB container and returns its type name as a human-readable string. It handles different JSONB container types including scalars, arrays, and objects. For scalar values, it delegates to JsonbTypeName to get the specific scalar type. For containers, it directly returns "array" or "object" based on the container type. If an invalid container type is encountered, it logs an error and returns "unknown".

## Parameters / Member Variables
- : Pointer to a JsonbContainer structure that needs type identification

## Dependencies
- Functions called/Symbols referenced:
  - [JsonbExtractScalar](JsonbExtractScalar.md)
  - [JsonbTypeName](JsonbTypeName.md)
  - JsonContainerIsArray
  - JsonContainerIsObject
  - elog
- Called from (representative examples):
  - [JsonbTypeName](JsonbTypeName.md)
  - [jsonb_typeof](../j/jsonb_typeof.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- The function handles error cases by logging an ERROR level message for invalid container types
- Used internally by PostgreSQL's JSONB type system for type introspection and debugging
- The function prioritizes scalar extraction first, then checks for array and object types