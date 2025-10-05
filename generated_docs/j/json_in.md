# json_in

## Location
[src/backend/utils/adt/json.c:105-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L105-L123)

## Overview
Converts a JSON string input into PostgreSQL's internal JSON text representation, performing validation during the conversion process.

## Definition

```c
Datum
json_in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the input conversion function for PostgreSQL's JSON data type. It takes a C-style string representation of JSON data and converts it into PostgreSQL's internal text format while ensuring the JSON is syntactically valid. This function is typically called when JSON data is being inserted into a table or passed as a parameter to a function that expects JSON input.

The function performs JSON validation using PostgreSQL's JSON parser with a null semantic action, meaning it parses the JSON structure to verify syntax correctness but doesn't perform any semantic processing of the content. If validation fails, the function returns NULL in a safe manner that doesn't raise an error.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0: C-style string () containing the JSON text to be validated and converted

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts C-string argument from function call
  - : Converts C-string to PostgreSQL text type
  - : Structure for JSON lexical context
  - : Initializes JSON lexical context for parsing
  - : Validates JSON syntax with error handling
  - : Semantic action that performs no operations during parsing
  - : Returns text datum to PostgreSQL
- Called from (representative examples):
  - PostgreSQL type input/output system when converting string literals to JSON
  - Direct function calls in SQL queries expecting JSON input

## Notes and Other Information
- The internal representation of JSON in PostgreSQL is identical to the text type, stored as variable-length text
- JSON validation is performed at input time to ensure only valid JSON is stored
- Uses error-safe parsing that returns NULL instead of throwing errors on invalid JSON when called in appropriate contexts
- The function is part of PostgreSQL's type input/output infrastructure and is automatically invoked during type conversions

## Simplified Source

```c
Datum
json_in(PG_FUNCTION_ARGS)
{
    char *json_string = PG_GETARG_CSTRING(0);
    text *result = cstring_to_text(json_string);
    JsonLexContext lex;

    // Validate JSON syntax
    makeJsonLexContext(&lex, result, false);
    if (!pg_parse_json_or_errsave(&lex, &nullSemAction, fcinfo->context))
        PG_RETURN_NULL();

    // Return as text (internal JSON representation)
    PG_RETURN_TEXT_P(result);
}
```