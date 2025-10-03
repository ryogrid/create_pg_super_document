# gettoken_query_standard

## Location
[src/backend/utils/adt/tsquery.c:286-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L286-L397)

## Overview
A static function that tokenizes and parses standard PostgreSQL tsquery expressions, implementing a finite state machine to recognize operators, operands, and syntactic elements.

## Definition

```c
static ts_tokentype
gettoken_query_standard(TSQueryParserState state, int8 *operator,
						int *lenval, char **strval,
						int16 *weight, bool *prefix)
```
## Detailed Description
The gettoken_query_standard function implements the core parsing logic for PostgreSQL's standard tsquery syntax. It uses a finite state machine with two main states (WAITFIRSTOPERAND/WAITOPERAND and WAITOPERATOR) to process input character by character. The function recognizes various token types including logical operators (&, |, \!), phrase operators (<N>), parentheses for grouping, and operands (lexemes with optional modifiers).

The parser leverages the tsvector parser for operand processing and supports weight modifiers (A, B, C, D) and prefix matching (*) through the get_modifiers helper function. It maintains proper parentheses balance and handles error conditions including syntax errors and malformed expressions.

## Parameters / Member Variables
- `state`: Parser state containing current position, parsing state, and context information
- `*operator`: Output parameter receiving the operator type (OP_AND, OP_OR, OP_NOT, OP_PHRASE)
- `*lenval`: Output parameter for the length of parsed string values
- `**strval`: Output parameter for parsed string values (operands)
- `*weight`: Output parameter for weight information or phrase distance
- `*prefix`: Output parameter indicating if prefix matching is enabled
## Dependencies
- Functions called/Symbols referenced:
  - [TSQueryParserState](../T/TSQueryParserState.md)
  - t_iseq
  - [t_isspace](../t/t_isspace.md)
  - [reset_tsvector_parser](../r/reset_tsvector_parser.md)
  - [gettoken_tsvector](gettoken_tsvector.md)
  - [get_modifiers](get_modifiers.md)
  - parse_phrase_operator
  - SOFT_ERROR_OCCURRED
  - ereturn
  - [pg_mblen](../p/pg_mblen.md)
- Called from (representative examples):
  - [parse_tsquery](../p/parse_tsquery.md)

## Notes and Other Information
- Implements standard PostgreSQL tsquery syntax parsing (not websearch style)
- Uses state machine with WAITFIRSTOPERAND, WAITOPERAND, and WAITOPERATOR states
- Supports all standard tsquery operators: & (AND), | (OR), \! (NOT), <N> (PHRASE)
- Handles nested expressions with parentheses and maintains balance counting
- Integrates with tsvector parser for consistent lexeme processing
- Returns different PT_* token types: PT_VAL, PT_OPR, PT_OPEN, PT_CLOSE, PT_END, PT_ERR
- Weight parameter is reused to store phrase distance when parsing phrase operators