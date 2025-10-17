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
- `*curitem`: Pointer to the current QueryItem being processed in the query tree
- `*arg`: Opaque argument passed through to the TSExecuteCallback function
- `flags`: Execution control flags including TS_EXEC_SKIP_NOT and TS_EXEC_PHRASE_NO_POS
- `chkcond`: Callback function to check whether a primitive lexeme value is present
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - CHECK_FOR_INTERRUPTS
  - chkcond (callback)
  - [TS_phrase_execute](TS_phrase_execute.md)
  - elog
- Called from (representative examples):
  - [TS_execute](TS_execute.md)
  - [TS_execute_ternary](TS_execute_ternary.md)
  - [TS_execute_recurse](TS_execute_recurse.md) (recursive calls)

## Notes and Other Information
- Operates as a position-agnostic evaluation engine until phrase operators are reached
- Uses short-circuit evaluation to optimize performance in boolean operations
- The recursive nature requires stack depth monitoring to prevent overflow
- Critical distinction: operates without position tracking unlike TS_phrase_execute
- The conversion of TS_MAYBE results at phrase boundaries maintains backward compatibility
- Essential component of PostgreSQL's full-text search infrastructure, bridging high-level boolean logic with position-aware phrase matching

## Simplified Source

```c
static TSTernaryValue TS_execute_recurse(QueryItem *curitem, void *arg, uint32 flags,
                                        TSExecuteCallback chkcond) {
    TSTernaryValue lmatch;

    // Safety checks
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    // Base case: evaluate leaf operand
    if (curitem->type == QI_VAL)
        return chkcond(arg, (QueryOperand *) curitem, NULL /* no position info needed */);

    // Handle different operators
    switch (curitem->qoperator.oper) {
        case OP_NOT:
            if (flags & TS_EXEC_SKIP_NOT)
                return TS_YES;

            // Invert the result of the operand
            switch (TS_execute_recurse(curitem + 1, arg, flags, chkcond)) {
                case TS_NO:   return TS_YES;
                case TS_YES:  return TS_NO;
                case TS_MAYBE: return TS_MAYBE;
            }
            break;

        case OP_AND:
            // Short-circuit: if left is NO, result is NO
            lmatch = TS_execute_recurse(curitem + curitem->qoperator.left, arg, flags, chkcond);
            if (lmatch == TS_NO)
                return TS_NO;

            // Result depends on right operand
            switch (TS_execute_recurse(curitem + 1, arg, flags, chkcond)) {
                case TS_NO:    return TS_NO;
                case TS_YES:   return lmatch;  // Return left result (YES or MAYBE)
                case TS_MAYBE: return TS_MAYBE;
            }
            break;

        case OP_OR:
            // Short-circuit: if left is YES, result is YES
            lmatch = TS_execute_recurse(curitem + curitem->qoperator.left, arg, flags, chkcond);
            if (lmatch == TS_YES)
                return TS_YES;

            // Result depends on right operand
            switch (TS_execute_recurse(curitem + 1, arg, flags, chkcond)) {
                case TS_NO:    return lmatch;  // Return left result (NO or MAYBE)
                case TS_YES:   return TS_YES;
                case TS_MAYBE: return TS_MAYBE;
            }
            break;

        case OP_PHRASE:
            // Delegate to phrase execution logic
            switch (TS_phrase_execute(curitem, arg, flags, chkcond, NULL)) {
                case TS_NO:    return TS_NO;
                case TS_YES:   return TS_YES;
                case TS_MAYBE:
                    // Convert MAYBE to NO unless caller wants MAYBE results
                    return (flags & TS_EXEC_PHRASE_NO_POS) ? TS_MAYBE : TS_NO;
            }
            break;

        default:
            elog(ERROR, "unrecognized operator: %d", curitem->qoperator.oper);
    }

    return TS_NO;
}
```