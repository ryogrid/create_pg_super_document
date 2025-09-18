# pushOperator

## Location
src/backend/utils/adt/tsquery.c: 531 - 546

## Overview
Pushes a query operator onto the parser state's polish notation stack during tsquery parsing, handling logical and phrase operators with optional distance parameters.

## Definition
```c
void
pushOperator(TSQueryParserState state, int8 oper, int16 distance)
```

## Detailed Description
This function creates and pushes a query operator onto the polish notation string (polstr) stack within the TSQueryParserState. It's a core component of the tsquery parsing system that handles the construction of query trees in polish notation format. The function supports four types of operators: NOT, AND, OR, and PHRASE, with special handling for phrase operators that can include distance parameters for proximity searching.

The function allocates memory for a new QueryOperator structure, initializes its fields appropriately, and adds it to the front of the polstr list using lcons. For phrase operators, it preserves the distance parameter; for other operators, the distance is set to 0. The left operand reference is filled in later during query processing by the findoprnd function.

## Parameters / Member Variables
- `state`: TSQueryParserState containing the current parsing context and polish notation stack
- `oper`: The operator type to push (OP_NOT, OP_AND, OP_OR, or OP_PHRASE)
- `distance`: Distance parameter for phrase operators (ignored for other operator types)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (PostgreSQL memory allocation function)
  - lcons (list construction function)
  - QueryOperator (query operator structure type)
  - QI_OPR (query item type constant)
  - OP_NOT, OP_AND, OP_OR, OP_PHRASE (operator type constants)
- Called from (representative examples):
  - pushval_morph
  - cleanOpStack
  - P_TSQ_WEB

## Notes and Other Information
- This function is part of PostgreSQL's text search query parsing infrastructure
- The function includes an assertion to validate that only supported operator types are passed
- Memory allocation uses palloc0 to ensure the structure is zero-initialized
- The distance parameter is only meaningful for OP_PHRASE operators and is ignored for other types
- The polstr (polish string) represents the query in postfix notation for efficient evaluation
- The left operand field is intentionally left uninitialized and filled in later during query tree construction