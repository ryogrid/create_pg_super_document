# shortest

## Location
src/backend/regex/rege_dfa.c: 204 - 370

## Overview
Implements the shortest-preferred matching engine for DFA-based regular expression matching in PostgreSQL.

## Definition
```c
static chr *
shortest(struct vars *v,
         struct dfa *d,
         chr *start,       /* where the match should start */
         chr *min,         /* match must end at or after here */
         chr *max,         /* match must end at or before here */
         chr **coldp,      /* store coldstart pointer here, if non-NULL */
         int *hitstopp)    /* record whether hit v->stop, if non-NULL */
```

## Detailed Description
The `shortest` function implements shortest-preferred matching for DFA-based regex execution. Unlike `longest`, it stops as soon as it finds the first valid match within the specified range. It processes text character by character while maintaining DFA state sets, but breaks out of the scanning loop immediately when a POSTSTATE (accepting state) is reached and the minimum match length is satisfied. The function includes optimizations for backref patterns and MATCHALL NFAs, and provides coldstart information for optimization purposes.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and state
- `d`: Pointer to DFA structure containing the compiled automaton
- `start`: Starting character position where matching should begin
- `min`: Minimum ending position - match must end at or after this point
- `max`: Maximum ending position - match must end at or before this point
- `coldp`: Optional pointer to store the last no-progress state set location
- `hitstopp`: Optional pointer to record whether matching hit the global stop position

## Dependencies
- Functions called/Symbols referenced:
  - [dfa_backref](../d/dfa_backref.md) (for handling backreferences)
  - [initialize](../i/initialize.md) (for setting up initial DFA state)
  - [miss](../m/miss.md) (for handling state transitions)
  - lastcold (for coldstart optimization)
  - GETCOLOR (for character-to-color mapping)
  - FDEBUG (for debug tracing)
- Called from (representative examples):
  - [dfa_backref](../d/dfa_backref.md) (recursive calls for backref processing)
  - [lacon](../l/lacon.md) (lookahead/lookbehind processing)
  - LOFF (regex execution offset function)
  - find, cfindloop (main search functions)

## Notes and Other Information
- Prioritizes finding the shortest valid match rather than the longest
- Contains early termination logic when POSTSTATE is reached within bounds
- Handles complex boundary conditions between min/max positions
- Supports both coldstart optimization and hitStop tracking
- Critical for non-greedy quantifiers and minimal matching patterns in PostgreSQL regex engine