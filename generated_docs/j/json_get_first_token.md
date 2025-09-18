# json_get_first_token

## Location
src/backend/utils/adt/jsonfuncs.c: 5948 - 5974

## Overview
Extracts and returns the type of the first JSON token from a text input, providing a quick way to determine the JSON value type without full parsing.

## Definition
```c
JsonTokenType json_get_first_token(text *json, bool throw_error)
```

## Detailed Description
This function performs a lightweight JSON analysis by lexing only the first token from the provided JSON text input. It's designed for scenarios where you need to quickly determine what type of JSON value you're dealing with (object, array, string, number, boolean, null) without the overhead of parsing the entire JSON structure.

The function initializes a JSON lexical context, performs a single lexical analysis step, and returns the token type. If parsing fails and throw_error is true, it will report the error through the PostgreSQL error system. Otherwise, it returns JSON_TOKEN_INVALID to indicate invalid JSON.

This is particularly useful for JSON type checking operations, conditional processing based on JSON value type, and validation scenarios where you only need to know the JSON's top-level structure.

## Parameters / Member Variables
- `json`: PostgreSQL text object containing the JSON string to analyze
- `throw_error`: Boolean flag indicating whether to throw an error on invalid JSON (true) or return an invalid token type (false)

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (lexical context structure for JSON parsing)
  - JsonParseErrorType (enum for JSON parsing error types)
  - makeJsonLexContext (function to initialize JSON lexical context)
  - json_lex (function to perform lexical analysis of JSON)
  - JSON_SUCCESS (success return code)
  - json_errsave_error (function to report JSON parsing errors)
  - JSON_TOKEN_INVALID (token type for invalid JSON)
- Called from (representative examples):
  - ExecEvalJsonIsPredicate (JSON predicate evaluation in executor)
  - pg_parse_json_or_ereport (JSON parsing utility function)

## Notes and Other Information
- Non-static function, indicating it's part of the public JSON API for PostgreSQL
- Performs minimal parsing - only the first token, making it efficient for type checking
- The throw_error parameter allows for different error handling strategies depending on context
- Returns JsonTokenType values like JSON_TOKEN_OBJECT_START, JSON_TOKEN_ARRAY_START, JSON_TOKEN_STRING, etc.
- Does not validate the entire JSON structure, only the first token
- Used in JSON type predicate functions and conditional JSON processing
- Part of PostgreSQL's JSON infrastructure for lightweight type inspection
- The returned token type can be used to branch to appropriate JSON processing logic