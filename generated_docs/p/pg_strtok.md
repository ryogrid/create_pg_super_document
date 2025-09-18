# pg_strtok

## Location
src/backend/nodes/read.c: 153 - 213

## Overview
A specialized tokenizer function that parses string representations of PostgreSQL Node trees, extracting individual tokens while preserving backslash escaping.

## Definition


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
  - nodeRead
  - READ_INT_FIELD
  - READ_UINT_FIELD
  - READ_STRING_FIELD
  - READ_NODE_FIELD
  - _readBitmapset
  - _readConst
  - parseNodeString
  - readDatum

## Notes and Other Information
- Uses global state (pg_strtok_ptr) for parsing position, making it non-reentrant without careful state management
- Preserves backslashes in tokens - caller must use debackslash to remove escape sequences
- Special case: '<>' token returns non-NULL pointer but length 0
- Implements PostgreSQL-specific rules rather than configurable token delimiters
- Critical component of the Node serialization/deserialization infrastructure
- Designed to work with stringToNodeInternal's state management for re-entrant safety