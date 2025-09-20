# makepol

## Location
[src/backend/utils/adt/tsquery.c:672-725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L672-L725)

## Overview
The main function that converts tsquery expressions from infix notation to polish (prefix) notation using a recursive descent parser with operator precedence handling.

## Definition

```c
static void
makepol(TSQueryParserState state,
		PushFunction pushval,
		Datum opaque)
```
## Detailed Description
The makepol function is the core parser for converting tsquery expressions into polish (prefix) notation. It implements a recursive descent parser that processes tokens from the input stream, handling values, operators, and parentheses. The function maintains an operator stack to handle precedence and associativity rules, calling cleanOpStack and pushOpStack to manage operators properly. For nested expressions (parentheses), it recursively calls itself. The function works with a callback mechanism (pushval) to output parsed elements, allowing different consumers to process the parsed query structure. It includes comprehensive error handling for malformed queries and integrates with PostgreSQL's soft error reporting system.

## Parameters / Member Variables
- `state`: TSQueryParserState structure containing the parser state, token stream, and error context
- `pushval`: PushFunction callback used to output parsed values and operators
- `opaque`: Datum parameter passed through to the pushval callback for context

## Dependencies
- Functions called/Symbols referenced:
  - TSQueryParserState (parser state structure)
  - PushFunction (callback function type)
  - ts_tokentype (token type enumeration)
  - OperatorElement (operator stack element)
  - STACKDEPTH (maximum stack depth constant)
  - check_stack_depth (stack overflow protection)
  - [cleanOpStack](../c/cleanOpStack.md) (operator precedence handling)
  - [pushOpStack](../p/pushOpStack.md) (operator stack management)
  - PT_VAL, PT_OPR, PT_OPEN, PT_CLOSE, PT_END, PT_ERR (token type constants)
  - OP_OR (lowest precedence operator)
  - SOFT_ERROR_OCCURRED (error checking macro)
  - errsave (soft error reporting)
- Called from (representative examples):
  - [makepol](makepol.md) (recursive calls for parentheses)
  - [parse_tsquery](../p/parse_tsquery.md) (main entry point)

## Notes and Other Information
- Implements recursive descent parsing for tsquery expressions
- Handles operator precedence and associativity through stack operations
- Supports recursive parsing for parenthesized sub-expressions
- Integrates with PostgreSQL's soft error handling system for graceful error recovery
- Uses callback mechanism for flexible output handling
- Includes stack overflow protection via check_stack_depth()
- Converts infix notation to prefix (polish) notation as required by PostgreSQL's internal tsquery representation