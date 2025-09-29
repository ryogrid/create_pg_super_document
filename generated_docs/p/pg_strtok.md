# pg_strtok

## Location
[src/backend/nodes/read.c:153-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/read.c#L153-L213)

## Overview
A specialized tokenizer function that parses string representations of PostgreSQL Node trees, extracting individual tokens while preserving backslash escaping.

## Definition

```c
const char *
pg_strtok(int *length)
```
## Detailed Description
This function serves as the core tokenizer for PostgreSQL's string-to-node deserialization system. It operates similar to the standard C strtok function but with several important differences: it never modifies the source string, returns token length through a parameter, and implements PostgreSQL-specific tokenization rules.

The tokenizer recognizes several types of tokens:
- Whitespace (space, tab, newline) always separates tokens
- Special single-character tokens: '(', ')', '{', '}'
- Regular tokens consisting of characters up to whitespace or special characters
- Backslash escaping allows including whitespace and special characters in tokens
- Special '<>' token that returns length 0 to represent empty values

The function uses a global state pointer (pg_strtok_ptr) to track the current parsing position, making it suitable for incremental parsing of Node string representations. Backslashes in tokens are preserved and must be processed by the caller using debackslash if needed.

## Parameters / Member Variables
- : Output parameter that receives the length of the returned token (including any embedded backslashes)

## Dependencies
- Functions called/Symbols referenced:
  - pg_strtok_ptr (global state variable)
- Called from (representative examples):
  - [nodeRead](../n/nodeRead.md)
  - READ_INT_FIELD
  - READ_UINT_FIELD
  - READ_STRING_FIELD
  - READ_NODE_FIELD
  - [_readBitmapset](../r/_readBitmapset.md)
  - [_readConst](../r/_readConst.md)
  - [parseNodeString](parseNodeString.md)
  - [readDatum](../r/readDatum.md)

## Notes and Other Information
- Uses global state (pg_strtok_ptr) for parsing position, making it non-reentrant without careful state management
- Preserves backslashes in tokens - caller must use debackslash to remove escape sequences
- Special case: '<>' token returns non-NULL pointer but length 0
- Implements PostgreSQL-specific rules rather than configurable token delimiters
- Critical component of the Node serialization/deserialization infrastructure
- Designed to work with stringToNodeInternal's state management for re-entrant safety

## Simplified Source

```c
const char *
pg_strtok(int *length)
{
    const char *local_str;      // working pointer to string
    const char *ret_str;        // start of token to return

    local_str = pg_strtok_ptr;  // get current position

    // Skip whitespace
    while (*local_str == ' ' || *local_str == '\n' || *local_str == '\t')
        local_str++;

    // End of string?
    if (*local_str == '\0')
    {
        *length = 0;
        pg_strtok_ptr = local_str;
        return NULL;            // no more tokens
    }

    // Start of next token
    ret_str = local_str;

    // Single character special tokens
    if (*local_str == '(' || *local_str == ')' ||
        *local_str == '{' || *local_str == '}')
    {
        local_str++;
    }
    else
    {
        // Normal token - scan until delimiter or special char
        while (*local_str != '\0' &&
               *local_str != ' ' && *local_str != '\n' &&
               *local_str != '\t' &&
               *local_str != '(' && *local_str != ')' &&
               *local_str != '{' && *local_str != '}')
        {
            // Handle backslash escaping
            if (*local_str == '\\' && local_str[1] != '\0')
                local_str += 2;  // skip escaped character
            else
                local_str++;
        }
    }

    *length = local_str - ret_str;

    // Special case: "<>" becomes empty token
    if (*length == 2 && ret_str[0] == '<' && ret_str[1] == '>')
        *length = 0;

    pg_strtok_ptr = local_str;  // update position for next call

    return ret_str;
}
```