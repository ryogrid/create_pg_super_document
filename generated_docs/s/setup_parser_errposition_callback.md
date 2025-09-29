# setup_parser_errposition_callback

## Location
[src/backend/parser/parse_node.c:140-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_node.c#L140-L155)

## Overview
Sets up an error context callback to provide parse error location information for non-parser functions that may throw errors.

## Definition

```c
void
setup_parser_errposition_callback(ParseCallbackState *pcbstate,
								  ParseState *pstate, int location)
```
## Detailed Description
The  function establishes an error context stack entry that enables non-parser functions to report parser error locations. This is essential when the parser calls external functions that cannot reasonably be passed a ParseState but should still provide meaningful error location information.

The function initializes a ParseCallbackState structure with the necessary context information and pushes it onto the error context stack. When an error occurs in the called function, the callback mechanism can provide the original parse location even though the error originated outside the parser subsystem.

This mechanism is part of PostgreSQL's comprehensive error reporting system that ensures users receive precise error locations regardless of where in the call stack the actual error occurs.

## Parameters / Member Variables
- : Pointer to a ParseCallbackState structure that will store the callback context. Must be declared as a local variable by the caller.
- : ParseState containing the source text and parsing context needed for error location reporting.
- : Byte offset in the source text where the error should be reported if an error occurs.

## Dependencies
- Functions called/Symbols referenced:
  -  (callback state structure)
  -  (error callback function)
  -  (global error context stack)
- Called from (representative examples):
  -  (src/backend/parser/analyze.c:2290)
  -  (src/backend/parser/parse_clause.c:3426)
  -  (src/backend/parser/parse_clause.c:3561)
  -  (src/backend/parser/parse_coerce.c:304)
  -  (src/backend/parser/parse_func.c:264)
  -  (src/backend/parser/parse_relation.c:1423)

## Notes and Other Information
- Must be paired with  to properly cleanup the error context stack
- The ParseCallbackState variable must remain in scope until the callback is cancelled
- Usage pattern: setup callback → call potentially error-throwing function → cancel callback
- Enables precise error reporting even when errors occur in functions outside the parser subsystem
- Part of PostgreSQL's error context stack mechanism for comprehensive error location tracking
- Location: src/backend/parser/parse_node.c:140-155

## Simplified Source

```c
void setup_parser_errposition_callback(ParseCallbackState *pcbstate,
                                       ParseState *pstate, int location) {
    // Set up parser state and location for error reporting
    pcbstate->pstate = pstate;
    pcbstate->location = location;

    // Configure error callback function
    pcbstate->errcallback.callback = pcb_error_callback;
    pcbstate->errcallback.arg = (void *) pcbstate;

    // Push onto error context stack
    pcbstate->errcallback.previous = error_context_stack;
    error_context_stack = &pcbstate->errcallback;
}
```