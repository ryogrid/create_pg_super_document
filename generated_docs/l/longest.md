# longest

## Location
[src/backend/regex/rege_dfa.c:42-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L42-L203)

## Overview
Implements the longest-preferred matching engine for DFA-based regular expression matching in PostgreSQL.

## Definition
```c
static chr *
longest(struct vars *v,
        struct dfa *d,
        chr *start,        /* where the match should start */
        chr *stop,         /* match must end at or before here */
        int *hitstopp)     /* record whether hit v->stop, if non-NULL */
```

## Detailed Description
The `longest` function is the core longest-preferred matching engine for DFA-based regular expression execution. It processes input text character by character, maintaining DFA state sets and tracking the longest possible match. The function handles special cases including backref matching and "matchall" NFAs (patterns that match any character sequence). It uses an optimized main loop with optional tracing support for debugging and returns the endpoint of the longest match found, or NULL if no match exists.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and state
- `d`: Pointer to DFA structure containing the compiled automaton 
- `start`: Starting character position where matching should begin
- `stop`: Ending character position where matching must end at or before
- `hitstopp`: Optional pointer to record whether matching hit the global stop position

## Dependencies
- Functions called/Symbols referenced:
  - [dfa_backref](../d/dfa_backref.md) (for handling backreferences)
  - [initialize](../i/initialize.md) (for setting up initial DFA state)
  - [miss](../m/miss.md) (for handling state transitions)
  - GETCOLOR (for character-to-color mapping)
  - FDEBUG (for debug tracing)
- Called from (representative examples):
  - LOFF (regex execution offset function)
  - [find](../f/find.md) (main regex search function)
  - [cfindloop](../c/cfindloop.md) (complex find loop)
  - [ccondissect](../c/ccondissect.md), caltdissect, citerdissect (dissection functions)

## Notes and Other Information
- Contains specialized fast paths for backref patterns and MATCHALL NFAs
- Includes conditional debug tracing code that can be enabled via REG_FTRACE
- The main scanning loop is duplicated to avoid trace overhead in production
- Returns match endpoint on success, NULL on failure or no match
- Essential component of PostgreSQLs regex execution engine for longest-match semantics