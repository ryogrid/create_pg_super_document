# json_get_first_token

## Location
[src/backend/utils/adt/jsonfuncs.c:5948-5974](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5948-L5974)

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
  - [JsonLexContext](../J/JsonLexContext.md) (lexical context structure for JSON parsing)
  - JsonParseErrorType (enum for JSON parsing error types)
  - [makeJsonLexContext](../m/makeJsonLexContext.md) (function to initialize JSON lexical context)
  - [json_lex](json_lex.md) (function to perform lexical analysis of JSON)
  - JSON_SUCCESS (success return code)
  - [json_errsave_error](json_errsave_error.md) (function to report JSON parsing errors)
  - JSON_TOKEN_INVALID (token type for invalid JSON)
- Called from (representative examples):
  - [ExecEvalJsonIsPredicate](../E/ExecEvalJsonIsPredicate.md) (JSON predicate evaluation in executor)
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

## Simplified Source

```c
JsonTokenType json_get_first_token(text *json, bool throw_error) {
    JsonLexContext lex;
    JsonParseErrorType result;

    // Initialize lexical context for JSON parsing
    makeJsonLexContext(&lex, json, false);

    // Lex exactly one token from the input
    result = json_lex(&lex);

    if (result == JSON_SUCCESS)
        return lex.token_type;

    // Handle error based on caller preference
    if (throw_error)
        json_errsave_error(result, &lex, NULL);

    return JSON_TOKEN_INVALID;
}
```