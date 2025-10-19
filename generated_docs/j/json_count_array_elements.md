# json_count_array_elements

## Location
[src/common/jsonapi.c:588-649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L588-L649)

## Overview
A utility function that counts the number of elements in a JSON array by parsing through the array structure without processing the actual element values.

## Definition

```c
JsonParseErrorType
json_count_array_elements(JsonLexContext *lex, int *elements)
```
## Detailed Description
json_count_array_elements provides a specialized function for determining the number of elements in a JSON array. It operates by creating a shallow copy of the provided lexing context and parsing through the array structure, counting each element encountered. The function is designed to be called from array_start semantic action routines and safely uses a copy of the lexing context to avoid modifying the original parser state. It handles proper array syntax validation, including checking for opening/closing brackets and comma separators between elements.

## Parameters / Member Variables
- `*lex`: JsonLexContext pointer containing the current parsing state positioned at the start of an array token
- `*elements`: Pointer to integer that will receive the count of array elements found
## Dependencies
- Functions called/Symbols referenced:
  - [lex_expect](../l/lex_expect.md) (for array start/end token validation)
  - [lex_peek](../l/lex_peek.md) (for token lookahead)
  - [parse_array_element](../p/parse_array_element.md) (for processing individual array elements)
  - [json_lex](json_lex.md) (for advancing through tokens)
  - nullSemAction (global semantic action structure for element parsing)
- Called from (representative examples):
  - [get_array_start](../g/get_array_start.md) (src/backend/utils/adt/jsonfuncs.c:1311)

## Notes and Other Information
The function uses a shallow copy of the JsonLexContext to safely parse without affecting the original context state. It specifically sets the copied context's strval to NULL since element values are not needed for counting. The lexical level is incremented on the copy to properly track nesting depth during parsing. Empty arrays (containing no elements) are handled correctly and will result in a count of 0.

## Simplified Source

```c
JsonParseErrorType json_count_array_elements(JsonLexContext *lex, int *elements)
{
    JsonLexContext copylex;
    int count;
    JsonParseErrorType result;

    // Create safe copy of lexing context
    memcpy(&copylex, lex, sizeof(JsonLexContext));
    copylex.strval = NULL;  // Not interested in values
    copylex.lex_level++;

    count = 0;

    // Expect array start token
    result = lex_expect(JSON_PARSE_ARRAY_START, &copylex, JSON_TOKEN_ARRAY_START);
    if (result != JSON_SUCCESS)
        return result;

    // Count elements if array is not empty
    if (lex_peek(&copylex) != JSON_TOKEN_ARRAY_END) {
        while (1) {
            count++;

            // Parse array element (ignore value)
            result = parse_array_element(&copylex, &nullSemAction);
            if (result != JSON_SUCCESS)
                return result;

            // Break if no comma (end of elements)
            if (copylex.token_type != JSON_TOKEN_COMMA)
                break;

            // Advance past comma
            result = json_lex(&copylex);
            if (result != JSON_SUCCESS)
                return result;
        }
    }

    // Expect array end token
    result = lex_expect(JSON_PARSE_ARRAY_NEXT, &copylex, JSON_TOKEN_ARRAY_END);
    if (result != JSON_SUCCESS)
        return result;

    *elements = count;
    return JSON_SUCCESS;
}
```