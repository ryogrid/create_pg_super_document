# TS_execute_recurse

## Location
[src/backend/utils/adt/tsvector_op.c:1883-2006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L1883-L2006)

## Overview
TS_execute_recurse handles recursive execution of tsquery operators above phrase-level operations, focusing on boolean logic without requiring lexeme position tracking until OP_PHRASE operators are encountered.

## Definition

```c
static TSTernaryValue
TS_execute_recurse(QueryItem *curitem, void *arg, uint32 flags,
				   TSExecuteCallback chkcond)
```
## Detailed Description
This function implements the core recursive execution logic for PostgreSQL's text search query evaluation system. It handles all boolean operators (OP_NOT, OP_AND, OP_OR) while operating above the phrase level, meaning it doesn't need to track lexeme positions until it encounters OP_PHRASE operators.

Key operational characteristics:
- Recursively processes query trees for boolean operators
- Delegates phrase operations to TS_phrase_execute when OP_PHRASE is encountered
- Implements short-circuit evaluation for performance optimization
- Handles ternary logic (TS_YES, TS_NO, TS_MAYBE) throughout the evaluation
- Provides stack overflow protection and interrupt checking

Boolean logic implementation:
- OP_NOT: Inverts the result, with special handling for TS_EXEC_SKIP_NOT flag
- OP_AND: Returns TS_NO immediately if left operand is TS_NO, otherwise depends on right operand
- OP_OR: Returns TS_YES immediately if left operand is TS_YES, otherwise depends on right operand
- OP_PHRASE: Delegates to TS_phrase_execute with optional MAYBE-to-NO conversion

The function maintains bug-compatibility with older implementations by converting TS_MAYBE to TS_NO at the topmost phrase operator level when TS_EXEC_PHRASE_NO_POS flag is not set.

## Parameters / Member Variables
- : Pointer to the current QueryItem being processed in the query tree
- : Opaque argument passed through to the TSExecuteCallback function
- : Execution control flags including TS_EXEC_SKIP_NOT and TS_EXEC_PHRASE_NO_POS
- : Callback function to check whether a primitive lexeme value is present

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - CHECK_FOR_INTERRUPTS
  - chkcond (callback)
  - TS_phrase_execute
  - elog
- Called from (representative examples):
  - TS_execute
  - TS_execute_ternary
  - TS_execute_recurse (recursive calls)

## Notes and Other Information
- Operates as a position-agnostic evaluation engine until phrase operators are reached
- Uses short-circuit evaluation to optimize performance in boolean operations
- The recursive nature requires stack depth monitoring to prevent overflow
- Critical distinction: operates without position tracking unlike TS_phrase_execute
- The conversion of TS_MAYBE results at phrase boundaries maintains backward compatibility
- Essential component of PostgreSQL's full-text search infrastructure, bridging high-level boolean logic with position-aware phrase matching