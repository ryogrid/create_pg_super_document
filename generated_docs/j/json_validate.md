# json_validate

## Location
[src/backend/utils/adt/json.c:1664-1725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1664-L1725)

## Overview
A comprehensive JSON validation function that parses JSON text for syntactic correctness and optionally checks for unique object field names.

## Definition

```c
bool
json_validate(text *json, bool check_unique_keys, bool throw_error)
```
## Detailed Description
This function serves as PostgreSQL's primary JSON validation mechanism, performing both syntactic parsing and semantic validation of JSON text. It uses the core JSON parsing infrastructure (pg_parse_json) to verify that the input conforms to JSON syntax rules. When requested, it additionally performs uniqueness validation of object field names using a specialized callback system. The function can operate in two modes: returning a boolean result for programmatic use, or throwing errors for SQL context usage. The uniqueness checking utilizes a stack-based tracking system that monitors object nesting levels and maintains a hash table for efficient key collision detection.

## Parameters / Member Variables
- : Pointer to text structure containing the JSON string to validate
- : Boolean flag indicating whether to perform object field name uniqueness validation
- : Boolean flag controlling whether to throw PostgreSQL errors on validation failure or return false

## Dependencies
- Functions called/Symbols referenced:
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - [json_unique_check_init](json_unique_check_init.md)
  - [json_unique_object_start](json_unique_object_start.md)
  - [json_unique_object_field_start](json_unique_object_field_start.md)
  - [json_unique_object_end](json_unique_object_end.md)
  - [pg_parse_json](../p/pg_parse_json.md)
  - [json_errsave_error](json_errsave_error.md)
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
  - ereport
- Data types referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - [JsonSemAction](../J/JsonSemAction.md)
  - [JsonUniqueParsingState](../J/JsonUniqueParsingState.md)
  - JsonParseErrorType
  - JSON_SUCCESS
- Called from (representative examples):
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md)
  - [ExecEvalJsonIsPredicate](../E/ExecEvalJsonIsPredicate.md)

## Notes and Other Information
- Returns true for valid JSON, false for invalid JSON or non-unique keys
- The uniqueness checking is optional and controlled by the check_unique_keys parameter
- Uses PostgreSQL's standard error reporting mechanism when throw_error is true
- Implements efficient parsing through the pg_parse_json framework with custom semantic actions
- The function is part of PostgreSQL's JSON data type infrastructure and is used in JSON constructors and predicates
- Memory management includes proper cleanup of lexical context when uniqueness checking is enabled
- Error codes follow PostgreSQL conventions (ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE for duplicate keys)