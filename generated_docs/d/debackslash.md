# debackslash

## Location
[src/backend/nodes/read.c:214-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/read.c#L214-L229)

## Overview
A utility function that removes protective backslash escape sequences from tokens extracted by pg_strtok, creating a clean palloc'd string.

## Definition

```c
char *
debackslash(const char *token, int length)
```
## Detailed Description
This function processes tokens that contain backslash escape sequences, removing the protective backslashes that were used during tokenization to preserve special characters and whitespace. It creates a new palloc'd string containing the unescaped version of the input token.

The function operates by scanning through the input token character by character. When it encounters a backslash followed by another character, it skips the backslash and includes only the escaped character in the result. This effectively reverses the escaping process applied during tokenization.

The function is commonly used in conjunction with pg_strtok to process tokens that may contain escaped special characters, whitespace, or backslashes themselves. It ensures that the final string values used in Node structures contain the actual intended characters rather than their escaped representations.

## Parameters / Member Variables
- `*token`: Pointer to the input token string containing potential backslash escape sequences
- `length`: The length of the input token (as returned by pg_strtok)
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for memory allocation)
- Called from (representative examples):
  - [nodeRead](../n/nodeRead.md)
  - [nullable_string](../n/nullable_string.md)

## Notes and Other Information
- Creates a new palloc'd string that must be freed by the caller
- Handles the specific escaping rules used by pg_strtok
- Essential for converting escaped tokens back to their original character sequences
- Used primarily for string tokens that may contain special characters
- Part of PostgreSQL's Node deserialization infrastructure
- Simple but critical utility for proper token processing

## Simplified Source

```c
char *debackslash(const char *token, int length) {
    char *result = palloc(length + 1);
    char *ptr = result;

    // Process each character in the token
    while (length > 0) {
        // Skip escape backslash if followed by another character
        if (*token == '\\' && length > 1) {
            token++;
            length--;
        }

        // Copy the current character
        *ptr++ = *token++;
        length--;
    }

    // Null-terminate the result
    *ptr = '\0';
    return result;
}
```