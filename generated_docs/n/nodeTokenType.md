# nodeTokenType

## Location
[src/backend/nodes/read.c:246-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/read.c#L246-L319)

## Overview
Determines the type of a node token from its string representation, classifying it as one of several PostgreSQL node types or special tokens.

## Definition


## Detailed Description
The  function analyzes a token string and determines its semantic type based on its content and format. It performs lexical analysis to classify tokens into various categories that are meaningful for PostgreSQL's node system. The function handles numeric values (integers and floats), boolean values, quoted strings, bit strings, and special structural tokens like parentheses and braces.

The function uses pattern matching and syntax validation to distinguish between different token types. For numeric tokens, it performs both syntax checking and range validation using  to determine whether a numeric token should be classified as an integer or float. The classification is essential for proper deserialization of PostgreSQL's internal node structures.

## Parameters / Member Variables
- : Pointer to the string token to be analyzed
- : Length of the token string in characters

## Dependencies
- Functions called/Symbols referenced:
  - strtoint
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
- Boolean tokens must match exactly "true" or "false"
- String tokens are identified by surrounding double quotes
- Bit string tokens are identified by leading 'b' or 'x' characters
- Static function internal to the node reading subsystem