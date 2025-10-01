# parse_object_field

## Location
[src/common/jsonapi.c:1052-1113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1052-L1113)

## Overview
A recursive descent parsing function that processes JSON object field definitions consisting of a field name (key), colon separator, and field value.

## Definition

```c
struct_action ostart = sem->object_start;
```
## Detailed Description
parse_object_field handles the parsing of individual JSON object fields following the pattern "fieldname" : value. It validates that the field name is a string token, extracts and preserves the field name for semantic callbacks, processes the colon separator, and then recursively parses the field value which can be a scalar, object, or array. The function manages semantic action callbacks for both field start and field end events, passing the field name and null status information. It supports the complete JSON value grammar for field values and ensures proper syntax validation throughout the parsing process.

## Parameters / Member Variables
- : JsonLexContext pointer containing the current parsing state positioned at a field name token
- : JsonSemAction pointer containing object field start/end callback functions and semantic state

## Dependencies
- Functions called/Symbols referenced:
  - [lex_peek](../l/lex_peek.md) (for token lookahead and validation)
  - [report_parse_error](../r/report_parse_error.md) (for syntax error reporting)
  - [json_lex](../j/json_lex.md) (for token consumption)
  - [lex_expect](../l/lex_expect.md) (for colon separator validation)
  - [parse_object](parse_object.md) (for nested object values)
  - [parse_array](parse_array.md) (for array values)
  - [parse_scalar](parse_scalar.md) (for scalar values)
  - [pstrdup](pstrdup.md) (for field name string duplication)
- Called from (representative examples):
  - [parse_object](parse_object.md) (src/common/jsonapi.c:1153, 1159) - [when](../w/when.md) processing object field lists

## Notes and Other Information
The function follows JSON object field syntax strictly, requiring string field names followed by colon separators. Field names are extracted and preserved for callback functions when object field semantic actions are provided. The function determines null status by checking if the field value is a JSON_TOKEN_NULL before invoking callbacks. It recursively handles complex field values by delegating to appropriate parsing functions based on the value's leading token. The semantic callbacks receive consistent field name and null status information for both start and end events.

## Simplified Source

```c
static JsonParseErrorType
parse_object_field(JsonLexContext *lex, JsonSemAction *sem)
{
    char *fname = NULL;
    bool isnull;
    JsonTokenType tok;
    JsonParseErrorType result;

    // Expect field name (string token)
    if (lex_peek(lex) != JSON_TOKEN_STRING)
        return report_parse_error(JSON_PARSE_STRING, lex);

    // Extract field name for callbacks
    if ((sem->object_field_start != NULL || sem->object_field_end != NULL) && lex->strval != NULL)
        fname = pstrdup(lex->strval->data);

    result = json_lex(lex);
    if (result != JSON_SUCCESS)
        return result;

    // Expect colon separator
    result = lex_expect(JSON_PARSE_OBJECT_LABEL, lex, JSON_TOKEN_COLON);
    if (result != JSON_SUCCESS)
        return result;

    // Check if value is null
    tok = lex_peek(lex);
    isnull = (tok == JSON_TOKEN_NULL);

    // Call field start callback
    if (sem->object_field_start != NULL) {
        result = (*sem->object_field_start)(sem->semstate, fname, isnull);
        if (result != JSON_SUCCESS)
            return result;
    }

    // Parse field value based on type
    switch (tok) {
        case JSON_TOKEN_OBJECT_START:
            result = parse_object(lex, sem);
            break;
        case JSON_TOKEN_ARRAY_START:
            result = parse_array(lex, sem);
            break;
        default:
            result = parse_scalar(lex, sem);
    }

    if (result != JSON_SUCCESS)
        return result;

    // Call field end callback
    if (sem->object_field_end != NULL) {
        result = (*sem->object_field_end)(sem->semstate, fname, isnull);
        if (result != JSON_SUCCESS)
            return result;
    }

    return JSON_SUCCESS;
}
```