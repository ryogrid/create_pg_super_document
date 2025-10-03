# nodeTokenType

## Location
[src/backend/nodes/read.c:246-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/read.c#L246-L319)

## Overview
Determines the type of a node token from its string representation, classifying it as one of several PostgreSQL node types or special tokens.

## Definition

```c
static NodeTag
nodeTokenType(const char *token, int length)
```
## Detailed Description
The  function analyzes a token string and determines its semantic type based on its content and format. It performs lexical analysis to classify tokens into various categories that are meaningful for PostgreSQL's node system. The function handles numeric values (integers and floats), boolean values, quoted strings, bit strings, and special structural tokens like parentheses and braces.

The function uses pattern matching and syntax validation to distinguish between different token types. For numeric tokens, it performs both syntax checking and range validation using  to determine whether a numeric token should be classified as an integer or float. The classification is essential for proper deserialization of PostgreSQL's internal node structures.

## Parameters / Member Variables
- `*token`: Pointer to the string token to be analyzed
- `length`: Length of the token string in characters
## Dependencies
- Functions called/Symbols referenced:
  - [strtoint](../s/strtoint.md)
  - LEFT_PAREN
  - RIGHT_PAREN  
  - LEFT_BRACE
  - OTHER_TOKEN
- Called from (representative examples):
  - [nodeRead](nodeRead.md)

## Notes and Other Information
- Returns one of the valid NodeTags: T_Integer, T_Float, T_Boolean, T_String, T_BitString, or special tokens: RIGHT_PAREN, LEFT_PAREN, LEFT_BRACE, OTHER_TOKEN
- Assumes the ASCII representation of the input token is legal
- For numeric detection, handles optional leading '+' or '-' signs
- Uses errno and range checking via strtoint() to distinguish integers from floats
- Single-character structural tokens ('(', ')', '{') are handled as special cases
- [Boolean](../B/Boolean.md) tokens must match exactly "true" or "false"
- [String](../S/String.md) tokens are identified by surrounding double quotes
- Bit string tokens are identified by leading 'b' or 'x' characters
- Static function internal to the node reading subsystem

## Simplified Source

```c
static NodeTag nodeTokenType(const char *token, int length)
{
    NodeTag retval;
    const char *numptr;
    int numlen;

    // Check if the token is a number
    numptr = token;
    numlen = length;
    if (*numptr == '+' || *numptr == '-')
        numptr++, numlen--;

    if ((numlen > 0 && isdigit((unsigned char) *numptr)) ||
        (numlen > 1 && *numptr == '.' && isdigit((unsigned char) numptr[1])))
    {
        // Test if it's an integer or float using strtoint
        char *endptr;
        errno = 0;
        (void) strtoint(numptr, &endptr, 10);
        if (endptr != token + length || errno == ERANGE)
            return T_Float;
        return T_Integer;
    }

    // Check single-character structural tokens
    else if (*token == '(')
        retval = LEFT_PAREN;
    else if (*token == ')')
        retval = RIGHT_PAREN;
    else if (*token == '{')
        retval = LEFT_BRACE;
    // Check boolean literals
    else if ((length == 4 && strncmp(token, "true", 4) == 0) ||
             (length == 5 && strncmp(token, "false", 5) == 0))
        retval = T_Boolean;
    // Check quoted strings
    else if (*token == '"' && length > 1 && token[length - 1] == '"')
        retval = T_String;
    // Check bit strings (binary or hex)
    else if (*token == 'b' || *token == 'x')
        retval = T_BitString;
    else
        retval = OTHER_TOKEN;

    return retval;
}
```