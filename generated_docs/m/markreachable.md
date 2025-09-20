# markreachable

## Location
[src/backend/regex/regc_nfa.c:2999-3024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L2999-L3024)

## Overview
The markreachable function is a recursive utility function in PostgreSQL's regex engine that marks all states reachable from a given starting state within an NFA (Nondeterministic Finite Automaton).

## Definition

```c
static void
markreachable(struct nfa *nfa,
			  struct state *s,
			  struct state *okay,	/* consider only states with this mark */
			  struct state *mark)	/* the value to mark with */
```
## Detailed Description
This function performs a depth-first traversal of an NFA starting from a given state, marking all reachable states with a specific mark value. It only considers states that currently have the 'okay' mark and changes them to the new 'mark' value. The function is recursive and follows all outgoing arcs from each state to find all reachable states.

The function includes stack overflow protection by checking if the recursion depth becomes too deep using the STACK_TOO_DEEP macro. This is important since regex patterns can potentially create very deep NFAs that could exhaust the call stack.

The marking mechanism uses the tmp field of state structures to track which states have been processed, preventing infinite loops in cyclic NFAs and ensuring each state is processed only once.

## Parameters / Member Variables
- : Pointer to the NFA structure being processed
- : The current state from which to mark reachable states
- : Only states with this mark value will be considered for processing
- : The new mark value to assign to reachable states

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (stack overflow protection macro)
  - NERR (error reporting macro)
  - REG_ETOOBIG (error code for regex too complex)
  - [markreachable](markreachable.md) (recursive self-call)
- Called from (representative examples):
  - [cleanup](../c/cleanup.md) (src/backend/regex/regc_nfa.c:2975)
  - REPLACEARC macro (src/backend/regex/regcomp.c:220)

## Notes and Other Information
- This is a static function, only accessible within the regc_nfa.c file
- The function is tail-recursive and includes stack overflow protection
- Uses the tmp field of state structures for marking purposes
- Critical for NFA optimization and cleanup operations in the regex engine
- The recursive nature makes it suitable for traversing complex NFA graphs efficiently