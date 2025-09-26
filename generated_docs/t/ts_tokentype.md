# ts_tokentype

## Location
[src/backend/utils/adt/tsquery.c:58-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L58-L77)

## Overview
An enumeration type that defines the token types returned by tsquery tokenizer functions during parsing of text search query strings.

## Definition

```c
typedef ts_tokentype (*ts_tokenizer) (TSQueryParserState state, int8 *operator,
									  int *lenval, char **strval,
									  int16 *weight, bool *prefix);
```
## Detailed Description
The  enum is used by the PostgreSQL text search query parser to categorize tokens encountered while parsing tsquery input strings. It serves as the return type for tokenizer functions that implement different query syntaxes (standard and websearch). Each token type represents a different syntactic element in the query language, from operands and operators to parentheses and end-of-input markers.

This enumeration is central to the lexical analysis phase of tsquery parsing, where the input string is broken down into meaningful tokens before being processed into an internal query tree representation.

## Parameters / Member Variables
- : End of input token, indicates no more tokens to process
- : Error token, indicates a parsing error has occurred  
- : Value token, represents a term/word operand in the query
- : Operator token, represents logical operators (&, |, !, <->)
- : Open parenthesis token '('
- : Close parenthesis token ')'

## Dependencies
- Functions called/Symbols referenced:
  - int8 (used in ts_tokenizer function pointer)
  - [TSQueryParserState](../T/TSQueryParserState.md) (used in ts_tokenizer function pointer)

- Called from (representative examples):
  - [gettoken_query_standard](../g/gettoken_query_standard.md)
  - [gettoken_query_websearch](../g/gettoken_query_websearch.md)  
  - [parse_or_operator](../p/parse_or_operator.md)
  - [makepol](../m/makepol.md)

## Notes and Other Information
- This enum is defined in src/backend/utils/adt/tsquery.c and is primarily used internally by the tsquery parsing infrastructure
- The ts_tokenizer function pointer typedef uses this enum as its return type, providing a standardized interface for different tokenizer implementations
- When PT_ERR is returned, the error context should be checked to determine if a specific error message was set, otherwise a generic parse error should be reported
- The numeric values are meaningful for comparison and processing logic within the parser